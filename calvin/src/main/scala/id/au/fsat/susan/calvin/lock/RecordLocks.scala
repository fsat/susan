/*
 * Copyright 2018 Felix Satyaputra
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package id.au.fsat.susan.calvin.lock

import java.time.Instant
import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Terminated }
import id.au.fsat.susan.calvin.{ RecordId, RemoteMessage, StateTransition }

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object RecordLocks {
  val name = "transaction-locks"

  def props()(implicit transactionLockSettings: RecordLockSettings): Props =
    Props(new RecordLocks())

  /**
   * All messages to [[RecordLocks]] actor must extends this trait.
   */
  sealed trait Message

  /**
   * All input messages to [[RecordLocks]] actor must extends this trait.
   */
  private[calvin] trait RequestMessage extends RemoteMessage

  /**
   * All response messages from [[RecordLocks]] actor must extends this trait.
   */
  private[calvin] trait ResponseMessage extends RemoteMessage

  /**
   * All self message must extends this trait.
   */
  sealed trait InternalMessage extends Message

  /**
   * All messages indicating failure must extends this trait.
   */
  sealed trait FailureMessage extends Exception with Message

  /**
   * Self message to fake reply from CRDT - TODO: remove me
   */
  @Deprecated
  case object Tick extends RequestMessage with InternalMessage

  /**
   * Self message to processing pending lock requests
   */
  case object ProcessPendingRequests extends RequestMessage with InternalMessage

  /**
   * Id for a particular lock request.
   */
  case class RequestId(value: UUID)

  /**
   * The transaction lock for a particular record.
   */
  case class Lock(requestId: RequestId, recordId: RecordId, lockId: UUID, createdAt: Instant, returnDeadline: Instant)

  /**
   * Request to obtain a particular transaction lock.
   */
  case class LockGetRequest(requestId: RequestId, recordId: RecordId, timeoutObtain: FiniteDuration, timeoutReturn: FiniteDuration) extends RequestMessage

  /**
   * The reply if the lock is successfully obtained.
   */
  case class LockGetSuccess(lock: Lock) extends Message with ResponseMessage

  /**
   * The reply if the lock can't be obtained within the timeout specified by [[LockGetRequest]].
   */
  case class LockGetTimeout(request: LockGetRequest) extends FailureMessage with ResponseMessage

  /**
   * The reply if the max allowable lock request is exceeded.
   */
  case class LockGetRequestDropped(request: LockGetRequest) extends FailureMessage with ResponseMessage

  /**
   * The reply if there's an exception obtaining the lock.
   */
  case class LockGetFailure(request: LockGetRequest, cause: Throwable) extends FailureMessage with ResponseMessage

  /**
   * Sent to the caller which requests the lock when the lock has expired
   */
  case class LockExpired(lock: Lock) extends FailureMessage with ResponseMessage

  /**
   * Request to return a particular transaction lock.
   */
  case class LockReturnRequest(lock: Lock) extends RequestMessage

  /**
   * The reply if the lock is successfully returned.
   */
  case class LockReturnSuccess(lock: Lock) extends ResponseMessage

  /**
   * The reply if the lock is returned past it's return deadline.
   */
  case class LockReturnLate(lock: Lock) extends FailureMessage with ResponseMessage

  /**
   * The reply if there's an exception obtaining the lock.
   */
  case class LockReturnFailure(lock: Lock, cause: Throwable) extends FailureMessage with ResponseMessage

  /**
   * Wrapper class for [[RecordLocksStorage]] message.
   */
  case class RecordLocksStorageMessageWrapper(message: RecordLocksStorage.Message) extends RequestMessage

  case object GetState extends RequestMessage
  case class GetStateSuccess(state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest]) extends RequestMessage

  /**
   * Represents a pending request for transaction lock
   */
  case class PendingRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant)

  /**
   * Represents a running request which has had transaction lock assigned.
   */
  case class RunningRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant, lock: Lock)

  /**
   * Represents the current state of [[RecordLocks]] actor.
   */
  trait RecordLocksState
  case object LoadingState extends RecordLocksState
  case object IdleState extends RecordLocksState
  case object PendingLockObtainedState extends RecordLocksState
  case object LockedState extends RecordLocksState
  case object PendingLockExpiredState extends RecordLocksState
  case object PendingLockReturnedState extends RecordLocksState
  case object NextPendingRequestState extends RecordLocksState
}

/**
 * Responsible for transaction lock for a particular record.
 *
 * Before calling one or more operations to modify a certain record, the caller *MUST* call the [[RecordLocks]] to
 * obtain the lock associated to the entity to be modified.
 */
class RecordLocks()(implicit recordLockSettings: RecordLockSettings) extends Actor with ActorLogging {
  import RecordLocks._

  import recordLockSettings._
  import context.dispatcher

  private val storage = context.watch(createRecordLocksStorage())

  override def preStart(): Unit = {
    Seq(Tick, ProcessPendingRequests).foreach(context.system.scheduler.schedule(checkInterval, checkInterval, self, _))

    storage ! RecordLocksStorage.GetStateRequest(self)
  }

  override def receive: Receive = stateTransition(loading(Seq.empty))

  protected def createRecordLocksStorage(): ActorRef =
    context.actorOf(RecordLocksStorage.props, RecordLocksStorage.Name)

  private def stateTransition(currentState: StateTransition[RequestMessage]): Receive = {
    case v: RequestMessage =>
      val nextState = currentState.pf(v)
      context.become(stateTransition(if (nextState == StateTransition.stay) currentState else nextState))

    case v: RecordLocksStorage.Message =>
      val wrapper = RecordLocksStorageMessageWrapper(v)
      self ! wrapper

    case Terminated(`storage`) =>
      context.stop(self)
  }

  private def loading(pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(appendLockGetRequestToPending(LoadingState, pendingRequests, persist = false)(nextState = loading))
      .orElse(dropStalePendingRequests(LoadingState, pendingRequests, persist = false)(loading))
      .orElse(StateTransition {
        case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateSuccess(state, runningRequestLoaded, pendingRequestsLoaded)) =>
          val pendingRequestsAll = pendingRequestsLoaded ++ pendingRequests

          def nextPendingRequestOrIdle(): StateTransition[RequestMessage] =
            if (pendingRequestsAll.nonEmpty)
              nextPendingRequest(pendingRequestsAll)
            else
              idle()

          def withRunningRequestPresent(nextState: RunningRequest => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
            runningRequestLoaded match {
              case Some(v) => nextState(v)
              case _ =>
                log.warning(s"Invalid state loaded [$state] with no running request present")
                nextPendingRequestOrIdle()
            }

          state match {
            case LoadingState => nextPendingRequestOrIdle()
            case IdleState    => idle()
            case PendingLockObtainedState =>
              // Continue to locked state
              withRunningRequestPresent { runningRequest =>
                runningRequest.caller ! LockGetSuccess(runningRequest.lock)
                locked(runningRequest, pendingRequestsAll)
              }
            case LockedState              => withRunningRequestPresent(locked(_, pendingRequestsAll))
            case PendingLockExpiredState  => withRunningRequestPresent(pendingLockExpired(_, pendingRequestsAll))
            case PendingLockReturnedState => withRunningRequestPresent(pendingLockReturned(_, pendingRequestsAll))
            case NextPendingRequestState  => nextPendingRequest(pendingRequestsAll)
          }

        case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateFailure(_, message, error)) =>
          error match {
            case Some(e) => log.error(e, message)
            case _       => log.error(message)
          }

          // Retry
          context.system.scheduler.scheduleOnce(checkInterval) {
            storage ! RecordLocksStorage.GetStateRequest(self)
          }

          StateTransition.stay
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(LoadingState, None, pendingRequests)
          StateTransition.stay
      })
      .orElse(StateTransition {
        case Tick =>
          StateTransition.stay
      })

  private def idle(): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(StateTransition {
        case request @ LockGetRequest(requestId, recordId, _, timeoutReturn) =>
          val now = Instant.now()
          val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))
          val runningRequest = RunningRequest(sender, request, Instant.now(), lock)

          // TODO: move this into persistState
          storage ! RecordLocksStorage.UpdateStateRequest(self, PendingLockObtainedState, Some(runningRequest), Seq.empty)
          persistState(PendingLockObtainedState)(pendingLockObtained(runningRequest, Seq.empty))

        case ProcessPendingRequests =>
          // Nothing to do, everything clear
          StateTransition.stay

        case Tick =>
          // Nothing to do, everything clear
          StateTransition.stay
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(IdleState, None, Seq.empty)
          StateTransition.stay
      })

  private def pendingLockObtained(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(PendingLockObtainedState, runningRequest, pendingRequests, persist = true)(pendingLockObtained(runningRequest, _)))
      .orElse(dropStalePendingRequests(PendingLockObtainedState, runningRequest, pendingRequests, persist = true)(pendingLockObtained(runningRequest, _)))
      .orElse(StateTransition {
        case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(PendingLockObtainedState, Some(`runningRequest`), _)) =>
          runningRequest.caller ! LockGetSuccess(runningRequest.lock)
          locked(runningRequest, pendingRequests)
      })
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          persistState(PendingLockReturnedState, runningRequest, pendingRequests)(pendingLockReturned(runningRequest, pendingRequests))

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out
          //runningRequest.caller ! LockGetSuccess(runningRequest.lock)
          //persistState(LockedState, runningRequest, pendingRequests)(locked(runningRequest, pendingRequests))
          StateTransition.stay
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(PendingLockObtainedState, Some(runningRequest), pendingRequests)
          StateTransition.stay
      })

  private def locked(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(LockedState, runningRequest, pendingRequests, persist = true)(locked(runningRequest, _)))
      .orElse(dropStalePendingRequests(LockedState, runningRequest, pendingRequests, persist = true)(locked(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: persist lock returned + pending requests into state
          persistState(LockedState, runningRequest, pendingRequests)(pendingLockReturned(runningRequest, pendingRequests))

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.stay

        case Tick =>
          val now = Instant.now()

          def isExpired(runningRequest: RunningRequest): Boolean = {
            val deadline = runningRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
            now.isAfter(deadline)
          }

          if (isExpired(runningRequest))
            // TODO: persist lock expired + pending requests into state
            persistState(PendingLockExpiredState, runningRequest, pendingRequests)(pendingLockExpired(runningRequest, pendingRequests))
          else
            StateTransition.stay
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(LockedState, Some(runningRequest), pendingRequests)
          StateTransition.stay
      })

  private def pendingLockExpired(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(PendingLockExpiredState, runningRequest, pendingRequests, persist = true)(pendingLockExpired(runningRequest, _)))
      .orElse(dropStalePendingRequests(PendingLockExpiredState, runningRequest, pendingRequests, persist = true)(pendingLockExpired(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is expired
          val lockExpired = LockExpired(runningRequest.lock)

          sender() ! lockExpired
          runningRequest.caller ! lockExpired

          if (pendingRequests.isEmpty)
            persistState(IdleState)(idle())
          else
            persistState(NextPendingRequestState, pendingRequests)(nextPendingRequest(pendingRequests))

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out

          runningRequest.caller ! LockExpired(runningRequest.lock)

          if (pendingRequests.isEmpty)
            persistState(IdleState)(idle())
          else
            persistState(NextPendingRequestState, pendingRequests)(nextPendingRequest(pendingRequests))
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(PendingLockExpiredState, Some(runningRequest), pendingRequests)
          StateTransition.stay
      })

  private def pendingLockReturned(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(PendingLockReturnedState, runningRequest, pendingRequests, persist = true)(pendingLockReturned(runningRequest, _)))
      .orElse(dropStalePendingRequests(PendingLockReturnedState, runningRequest, pendingRequests, persist = true)(pendingLockReturned(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is returned
          val lockReturned = LockReturnSuccess(runningRequest.lock)

          sender() ! lockReturned
          runningRequest.caller ! lockReturned

          if (pendingRequests.isEmpty)
            persistState(IdleState)(idle())
          else
            persistState(NextPendingRequestState, pendingRequests)(nextPendingRequest(pendingRequests))

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out

          val now = Instant.now()
          val isLate = now.isAfter(runningRequest.lock.returnDeadline)
          val reply = if (isLate) LockReturnLate(runningRequest.lock) else LockReturnSuccess(runningRequest.lock)

          runningRequest.caller ! reply

          if (pendingRequests.isEmpty)
            persistState(IdleState)(idle())
          else
            persistState(NextPendingRequestState, pendingRequests)(nextPendingRequest(pendingRequests))
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(PendingLockReturnedState, Some(runningRequest), pendingRequests)
          StateTransition.stay
      })

  private def nextPendingRequest(pendingRequests: Seq[PendingRequest]): StateTransition[RequestMessage] =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(appendLockGetRequestToPending(NextPendingRequestState, pendingRequests, persist = true)(nextPendingRequest))
      .orElse(dropStalePendingRequests(NextPendingRequestState, pendingRequests, persist = false) { pendingRequestsAlive =>
        if (pendingRequestsAlive.isEmpty)
          persistState(IdleState)(idle())
        else {
          val pendingRequest = pendingRequestsAlive.head

          val requestId = pendingRequest.request.requestId
          val recordId = pendingRequest.request.recordId
          val timeoutReturn = pendingRequest.request.timeoutReturn

          val now = Instant.now()
          val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

          val runningRequest = RunningRequest(pendingRequest.caller, pendingRequest.request, pendingRequest.createdAt, lock)

          // TODO: move this into persistState
          storage ! RecordLocksStorage.UpdateStateRequest(self, PendingLockObtainedState, Some(runningRequest), pendingRequestsAlive.tail)
          persistState(PendingLockObtainedState, runningRequest, pendingRequestsAlive.tail)(pendingLockObtained(runningRequest, pendingRequestsAlive.tail))
        }
      })
      .orElse(StateTransition {
        case Tick =>
          StateTransition.stay
      })
      .orElse(StateTransition {
        case GetState =>
          sender() ! GetStateSuccess(NextPendingRequestState, None, pendingRequests)
          StateTransition.stay
      })

  private def rejectInvalidLockGetRequest: StateTransition[RequestMessage] = StateTransition {
    case v @ LockGetRequest(_, _, timeoutObtain, _) if timeoutObtain > maxTimeoutObtain =>
      sender() ! LockGetFailure(v, new IllegalArgumentException(s"The lock obtain timeout of [${timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      StateTransition.stay

    case v @ LockGetRequest(_, _, _, timeoutReturn) if timeoutReturn > maxTimeoutReturn =>
      sender() ! LockGetFailure(v, new IllegalArgumentException(s"The lock return timeout of [${timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      StateTransition.stay
  }

  private def rejectLockReturnRequest: StateTransition[RequestMessage] = StateTransition {
    case LockReturnRequest(lock) =>
      sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
      StateTransition.stay
  }

  private def appendLockGetRequestToPending(state: RecordLocksState, pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    appendLockGetRequestToPending(state, None, pendingRequests, persist)(nextState)

  private def appendLockGetRequestToPending(state: RecordLocksState, runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    appendLockGetRequestToPending(state, Some(runningRequest), pendingRequests, persist)(nextState)

  private def appendLockGetRequestToPending(state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] = StateTransition {
    case v: LockGetRequest =>
      val pendingRequest = PendingRequest(sender(), v, Instant.now())
      val pendingRequestsUpdated = pendingRequests :+ pendingRequest
      if (persist)
        persistState(state, runningRequest, pendingRequestsUpdated)(nextState(pendingRequestsUpdated))
      else
        nextState(pendingRequestsUpdated)
  }

  private def dropStalePendingRequests(state: RecordLocksState, pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    dropStalePendingRequests(state, None, pendingRequests, persist)(nextState)

  private def dropStalePendingRequests(state: RecordLocksState, runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    dropStalePendingRequests(state, Some(runningRequest), pendingRequests, persist)(nextState)

  private def dropStalePendingRequests(state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest], persist: Boolean)(nextState: Seq[PendingRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] = StateTransition {
    case ProcessPendingRequests =>

      def filterExpired(now: Instant, pendingRequests: Seq[PendingRequest]): (Seq[PendingRequest], Seq[PendingRequest], Seq[PendingRequest]) = {
        def timedOut(input: PendingRequest): Boolean = {
          val deadline = input.createdAt.plusNanos(input.request.timeoutObtain.toNanos)
          now.isAfter(deadline)
        }

        val pendingTimedOut = pendingRequests.filter(timedOut)
        val pendingAlive = pendingRequests.filterNot(timedOut)

        val pendingAliveSorted = pendingAlive.sortBy(_.createdAt)
        val pendingAliveKept = pendingAliveSorted.take(maxPendingRequests)
        val pendingAliveDropped = pendingAliveSorted.takeRight(pendingAliveSorted.length - maxPendingRequests)

        (pendingTimedOut, pendingAliveKept, pendingAliveDropped)
      }

      val now = Instant.now()
      val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now, pendingRequests)

      pendingTimedOut.foreach { v =>
        v.caller ! LockGetTimeout(v.request)
      }

      pendingAliveDropped.foreach { v =>
        v.caller ! LockGetRequestDropped(v.request)
      }

      if (persist)
        persistState(state, runningRequest, pendingAliveKept)(nextState(pendingAliveKept))
      else
        nextState(pendingAliveKept)
  }

  private def persistState(state: RecordLocksState)(nextState: => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    persistState(state, None, Seq.empty)(nextState)

  private def persistState(state: RecordLocksState, pendingRequests: Seq[PendingRequest])(nextState: => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    persistState(state, None, pendingRequests)(nextState)

  private def persistState(state: RecordLocksState, runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest])(nextState: => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    persistState(state, Some(runningRequest), pendingRequests)(nextState)

  private def persistState(state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest])(nextState: => StateTransition[RequestMessage]): StateTransition[RequestMessage] = {
    // TODO: not sure if persisting pending requests is a good idea
    // We need to persist running request to prevent:
    // - False positive, i.e. thinking transaction is successful while it's actually not
    // - Committed, unreplied transaction
    // But we don't need to persist pending request?
    //storage ! RecordLocksStorage.UpdateStateRequest(from = self, state, runningRequest, pendingRequests)
    nextState
  }
}
