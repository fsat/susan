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

package id.au.fsat.susan.calvin.lockdeprecate

import java.time.Instant
import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props, Terminated }
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage.UpdateStateSuccess
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
  private[calvin] sealed trait RequestMessage extends RemoteMessage

  /**
   * All response messages from [[RecordLocks]] actor must extends this trait.
   */
  private[calvin] sealed trait ResponseMessage extends RemoteMessage

  /**
   * All self message must extends this trait.
   */
  sealed trait InternalMessage extends Message

  /**
   * All messages indicating failure must extends this trait.
   */
  sealed trait FailureMessage extends Exception with Message

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
   * Internal message to check the lock expiry
   */
  case object LockExpiryCheck extends RequestMessage with InternalMessage

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

  case class SubscribeRequest(ref: ActorRef) extends RequestMessage
  case object SubscribeSuccess extends ResponseMessage
  case class UnsubscribeRequest(ref: ActorRef) extends RequestMessage
  case object UnsubscribeSuccess extends ResponseMessage

  case class StateChanged(state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest]) extends ResponseMessage

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
  trait RecordLocksStateToPersist extends RecordLocksState
  case object LoadingState extends RecordLocksState
  case object IdleState extends RecordLocksStateToPersist
  case object PendingLockObtainedState extends RecordLocksState
  case object LockedState extends RecordLocksStateToPersist
  case object PendingLockExpiredState extends RecordLocksStateToPersist
  case object PendingLockReturnedState extends RecordLocksStateToPersist
  case object NextPendingRequestState extends RecordLocksState

  trait RecordLocksStateData {
    def state: RecordLocksState
    def runningRequestOpt: Option[RunningRequest]
    def pendingRequests: Seq[PendingRequest]
    def subscribers: Set[ActorRef]
  }

  case class LoadingStateData(pendingRequests: Seq[PendingRequest] = Seq.empty, subscribers: Set[ActorRef] = Set.empty) extends RecordLocksStateData {
    val state: RecordLocksState = LoadingState
    val runningRequestOpt: Option[RunningRequest] = None

    def set(input: Seq[PendingRequest]): LoadingStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): LoadingStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): LoadingStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class IdleStateData(subscribers: Set[ActorRef] = Set.empty) extends RecordLocksStateData {
    val state: RecordLocksState = IdleState
    val runningRequestOpt: Option[RunningRequest] = None
    val pendingRequests: Seq[PendingRequest] = Seq.empty

    def +(subscriber: ActorRef): IdleStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): IdleStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class PendingLockObtainedStateData(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]) extends RecordLocksStateData {
    val state: RecordLocksState = PendingLockObtainedState
    val runningRequestOpt: Option[RunningRequest] = Some(runningRequest)

    def set(input: Seq[PendingRequest]): PendingLockObtainedStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): PendingLockObtainedStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): PendingLockObtainedStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class LockedStateData(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef], lockExpiryCheck: Cancellable) extends RecordLocksStateData {
    val state: RecordLocksState = LockedState
    val runningRequestOpt: Option[RunningRequest] = Some(runningRequest)

    def set(input: Seq[PendingRequest]): LockedStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): LockedStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): LockedStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class PendingLockExpiredStateData(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]) extends RecordLocksStateData {
    val state: RecordLocksState = PendingLockExpiredState
    val runningRequestOpt: Option[RunningRequest] = Some(runningRequest)

    def set(input: Seq[PendingRequest]): PendingLockExpiredStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): PendingLockExpiredStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): PendingLockExpiredStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class PendingLockReturnedStateData(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]) extends RecordLocksStateData {
    val state: RecordLocksState = PendingLockReturnedState
    val runningRequestOpt: Option[RunningRequest] = Some(runningRequest)

    def set(input: Seq[PendingRequest]): PendingLockReturnedStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): PendingLockReturnedStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): PendingLockReturnedStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }

  case class NextPendingRequestStateData(pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]) extends RecordLocksStateData {
    val state: RecordLocksState = NextPendingRequestState
    val runningRequestOpt: Option[RunningRequest] = None

    def set(input: Seq[PendingRequest]): NextPendingRequestStateData = copy(pendingRequests = input)
    def +(subscriber: ActorRef): NextPendingRequestStateData = copy(subscribers = subscribers + subscriber)
    def -(subscriber: ActorRef): NextPendingRequestStateData = copy(subscribers = subscribers.filterNot(_ == subscriber))
  }
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

  private val storage = context.watch(createRecordLocksStorage(context))

  override def preStart(): Unit = {
    context.system.scheduler.schedule(checkInterval, checkInterval, self, ProcessPendingRequests)
    storage ! RecordLocksStorage.GetStateRequest(self)
  }

  override def receive: Receive = stateTransition(loading(notify = false)(LoadingStateData()))

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

  private def loading(notify: Boolean)(implicit stateData: LoadingStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(rejectLockReturnRequest)
        .orElse(appendLockGetRequestToPending(v => loading(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(dropStalePendingRequests(v => loading(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => loading(notify = false)(stateData + v),
          remove = v => loading(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateNotFound) =>
            if (stateData.pendingRequests.isEmpty)
              idle(stateData.subscribers)
            else
              nextPendingRequest(stateData.pendingRequests, stateData.subscribers)
        })
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateSuccess(state, runningRequestLoaded)) =>
            import stateData.{ subscribers, pendingRequests }

            def nextPendingRequestOrIdle(): StateTransition[RequestMessage] =
              if (pendingRequests.nonEmpty)
                nextPendingRequest(pendingRequests, subscribers)
              else
                idle(subscribers)

            def withRunningRequestPresent(nextState: RunningRequest => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
              runningRequestLoaded match {
                case Some(v) => nextState(v)
                case _ =>
                  log.warning(s"Invalid state loaded [$state] with no running request present")
                  nextPendingRequestOrIdle()
              }

            state match {
              case IdleState   => if (pendingRequests.isEmpty) idle(subscribers) else nextPendingRequest(pendingRequests, subscribers)
              case LockedState => withRunningRequestPresent(locked(_, pendingRequests, subscribers))
              case PendingLockExpiredState =>
                withRunningRequestPresent { runningRequest =>
                  notifySubscribers(subscribers, PendingLockExpiredState, Some(runningRequest), pendingRequests)

                  runningRequest.caller ! LockExpired(runningRequest.lock)

                  notifySubscribers(notify = true) {
                    if (pendingRequests.isEmpty) {
                      persistState(IdleState)
                      idle(subscribers)
                    } else
                      nextPendingRequest(pendingRequests, subscribers)
                  }
                }
              case PendingLockReturnedState =>
                withRunningRequestPresent { runningRequest =>
                  notifySubscribers(subscribers, PendingLockReturnedState, Some(runningRequest), pendingRequests)

                  val now = Instant.now()
                  val isLate = now.isAfter(runningRequest.lock.returnDeadline)
                  val reply = if (isLate) LockReturnLate(runningRequest.lock) else LockReturnSuccess(runningRequest.lock)

                  runningRequest.caller ! reply

                  notifySubscribers(notify = true) {
                    if (pendingRequests.isEmpty) {
                      persistState(IdleState)
                      idle(subscribers)
                    } else
                      nextPendingRequest(pendingRequests, subscribers)
                  }
                }
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
    }

  private def idle(subscribers: Set[ActorRef]): StateTransition[RequestMessage] = idle(notify = true)(IdleStateData(subscribers))

  private def idle(notify: Boolean)(implicit stateData: IdleStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(rejectLockReturnRequest)
        .orElse(handleSubscribers(
          append = v => idle(notify = false)(stateData + v),
          remove = v => idle(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case request @ LockGetRequest(requestId, recordId, _, timeoutReturn) =>
            val now = Instant.now()
            val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))
            val runningRequest = RunningRequest(sender, request, Instant.now(), lock)

            // TODO: move this into persistState
            persistState(LockedState, runningRequest)
            pendingLockObtained(runningRequest, Seq.empty, stateData.subscribers)

          case ProcessPendingRequests =>
            // Nothing to do, everything clear
            StateTransition.stay
        })
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(UpdateStateSuccess(IdleState, _)) =>
            StateTransition.stay
        })
    }

  private def pendingLockObtained(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]): StateTransition[RequestMessage] =
    pendingLockObtained(notify = true)(PendingLockObtainedStateData(runningRequest, pendingRequests, subscribers))

  private def pendingLockObtained(notify: Boolean)(implicit stateData: PendingLockObtainedStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(acknowledgeIdleStatePersisted)
        .orElse(appendLockGetRequestToPending(v => pendingLockObtained(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(dropStalePendingRequests(v => pendingLockObtained(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => pendingLockObtained(notify = false)(stateData + v),
          remove = v => pendingLockObtained(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest))) if runningRequest == stateData.runningRequest =>
            runningRequest.caller ! LockGetSuccess(runningRequest.lock)
            locked(runningRequest, stateData.pendingRequests, stateData.subscribers)
        })
        .orElse(StateTransition {
          case LockReturnRequest(lock) if lock == stateData.runningRequest.lock =>
            val nextStateData = PendingLockReturnedStateData(stateData.runningRequest, stateData.pendingRequests, stateData.subscribers)
            persistState(PendingLockReturnedState, nextStateData.runningRequest)
            pendingLockReturned(notify = true)(nextStateData)

          case LockReturnRequest(lock) =>
            sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
            StateTransition.stay
        })
    }

  private def locked(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]): StateTransition[RequestMessage] =
    locked(notify = true)(
      LockedStateData(
        runningRequest,
        pendingRequests,
        subscribers,
        context.system.scheduler.schedule(checkInterval, checkInterval, self, LockExpiryCheck)))

  private def locked(notify: Boolean)(implicit stateData: LockedStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(acknowledgeIdleStatePersisted)
        .orElse(appendLockGetRequestToPending(v => locked(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(dropStalePendingRequests(v => locked(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => locked(notify = false)(stateData + v),
          remove = v => locked(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case LockReturnRequest(lock) if lock == stateData.runningRequest.lock =>
            stateData.lockExpiryCheck.cancel()

            val nextStateData = PendingLockReturnedStateData(stateData.runningRequest, stateData.pendingRequests, stateData.subscribers)
            persistState(PendingLockReturnedState, nextStateData.runningRequest)
            pendingLockReturned(notify = true)(nextStateData)

          case LockReturnRequest(lock) =>
            sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
            StateTransition.stay

          case LockExpiryCheck =>
            val now = Instant.now()

            def isExpired(runningRequest: RunningRequest): Boolean = {
              val deadline = runningRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
              now.isAfter(deadline)
            }

            if (isExpired(stateData.runningRequest)) {
              stateData.lockExpiryCheck.cancel()

              persistState(PendingLockExpiredState, stateData.runningRequest)
              pendingLockExpired(stateData.runningRequest, stateData.pendingRequests, stateData.subscribers)
            } else
              StateTransition.stay
        })
    }

  private def pendingLockExpired(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]): StateTransition[RequestMessage] =
    pendingLockExpired(notify = true)(PendingLockExpiredStateData(runningRequest, pendingRequests, subscribers))

  private def pendingLockExpired(notify: Boolean)(implicit stateData: PendingLockExpiredStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(acknowledgeIdleStatePersisted)
        .orElse(appendLockGetRequestToPending(v => pendingLockExpired(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(dropStalePendingRequests(v => pendingLockExpired(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => pendingLockExpired(notify = false)(stateData + v),
          remove = v => pendingLockExpired(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(UpdateStateSuccess(PendingLockExpiredState, Some(r))) if r == stateData.runningRequest =>
            // TODO: cancel running requests if timed out

            import stateData._

            runningRequest.caller ! LockExpired(runningRequest.lock)

            if (pendingRequests.isEmpty) {
              persistState(IdleState)
              idle(subscribers)
            } else
              nextPendingRequest(pendingRequests, subscribers)
        })
        .orElse(StateTransition {
          case LockReturnRequest(lock) if lock == stateData.runningRequest.lock =>
            import stateData._

            // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is expired
            val lockExpired = LockExpired(runningRequest.lock)

            sender() ! lockExpired
            runningRequest.caller ! lockExpired

            if (pendingRequests.isEmpty) {
              persistState(IdleState)
              idle(subscribers)
            } else
              nextPendingRequest(pendingRequests, subscribers)

          case LockReturnRequest(lock) =>
            sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
            StateTransition.stay
        })
    }
  private def pendingLockReturned(notify: Boolean)(implicit stateData: PendingLockReturnedStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(acknowledgeIdleStatePersisted)
        .orElse(appendLockGetRequestToPending(v => pendingLockReturned(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(dropStalePendingRequests(v => pendingLockReturned(notify = v != stateData.pendingRequests)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => pendingLockReturned(notify = false)(stateData + v),
          remove = v => pendingLockReturned(notify = false)(stateData - v)))
        .orElse(StateTransition {
          case RecordLocksStorageMessageWrapper(UpdateStateSuccess(PendingLockReturnedState, Some(r))) if r == stateData.runningRequest =>
            import stateData._

            // TODO: cancel running requests if timed out

            val now = Instant.now()
            val isLate = now.isAfter(runningRequest.lock.returnDeadline)
            val reply = if (isLate) LockReturnLate(runningRequest.lock) else LockReturnSuccess(runningRequest.lock)

            runningRequest.caller ! reply

            if (pendingRequests.isEmpty) {
              persistState(IdleState)
              idle(subscribers)
            } else
              nextPendingRequest(pendingRequests, subscribers)

        })
        .orElse(StateTransition {
          case LockReturnRequest(lock) if lock == stateData.runningRequest.lock =>
            import stateData._

            // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is returned
            val lockReturned = LockReturnSuccess(runningRequest.lock)

            sender() ! lockReturned
            runningRequest.caller ! lockReturned

            if (pendingRequests.isEmpty) {
              persistState(IdleState)
              idle(subscribers)
            } else
              nextPendingRequest(pendingRequests, subscribers)

          case LockReturnRequest(lock) =>
            sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
            StateTransition.stay
        })
    }

  private def nextPendingRequest(pendingRequests: Seq[PendingRequest], subscribers: Set[ActorRef]): StateTransition[RequestMessage] =
    nextPendingRequest(notify = true)(NextPendingRequestStateData(pendingRequests, subscribers))

  private def nextPendingRequest(notify: Boolean)(implicit stateData: NextPendingRequestStateData): StateTransition[RequestMessage] =
    notifySubscribers(notify) {
      rejectInvalidLockGetRequest
        .orElse(rejectLockReturnRequest)
        .orElse(acknowledgeIdleStatePersisted)
        .orElse(appendLockGetRequestToPending(v => nextPendingRequest(notify = true)(stateData.set(v))))
        .orElse(handleSubscribers(
          append = v => nextPendingRequest(notify = false)(stateData + v),
          remove = v => nextPendingRequest(notify = false)(stateData - v)))
        .orElse(dropStalePendingRequests { pendingRequestsAlive =>
          import stateData.subscribers

          if (pendingRequestsAlive.isEmpty) {
            persistState(IdleState)
            idle(subscribers)
          } else {
            val pendingRequest = pendingRequestsAlive.head

            val requestId = pendingRequest.request.requestId
            val recordId = pendingRequest.request.recordId
            val timeoutReturn = pendingRequest.request.timeoutReturn

            val now = Instant.now()
            val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

            val runningRequest = RunningRequest(pendingRequest.caller, pendingRequest.request, pendingRequest.createdAt, lock)

            persistState(LockedState, runningRequest)
            pendingLockObtained(runningRequest, pendingRequestsAlive.tail, subscribers)
          }
        })
    }

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

  private def acknowledgeIdleStatePersisted: StateTransition[RequestMessage] = StateTransition {
    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(IdleState, _)) =>
      // We can arrive in this state if there's race between persisting the idle state and some other operation,
      // i.e. the lock request being sent.
      // This is an expected race condition, so just accept the idle state persisted confirmation and continue
      StateTransition.stay
  }

  private def handleSubscribers(append: ActorRef => StateTransition[RequestMessage], remove: ActorRef => StateTransition[RequestMessage])(implicit stateData: RecordLocksStateData): StateTransition[RequestMessage] =
    StateTransition {
      case SubscribeRequest(ref) =>
        ref ! SubscribeSuccess
        ref ! StateChanged(stateData.state, stateData.runningRequestOpt, stateData.pendingRequests)
        append(ref)

      case UnsubscribeRequest(ref) =>
        ref ! UnsubscribeSuccess
        remove(ref)
    }

  private def appendLockGetRequestToPending(nextState: Seq[PendingRequest] => StateTransition[RequestMessage])(implicit stateData: RecordLocksStateData): StateTransition[RequestMessage] =
    StateTransition {
      case v: LockGetRequest =>
        import stateData.pendingRequests

        nextState(pendingRequests :+ PendingRequest(sender(), v, Instant.now()))
    }

  private def dropStalePendingRequests(nextState: Seq[PendingRequest] => StateTransition[RequestMessage])(implicit stateData: RecordLocksStateData): StateTransition[RequestMessage] =
    StateTransition {
      case ProcessPendingRequests =>
        import stateData.pendingRequests

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

        nextState(pendingAliveKept)
    }

  private def persistState(state: RecordLocksStateToPersist): Unit =
    persistState(state, None)

  private def persistState(state: RecordLocksStateToPersist, runningRequest: RunningRequest): Unit =
    persistState(state, Some(runningRequest))

  private def persistState(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]): Unit = {
    // TODO: not sure if persisting pending requests is a good idea
    // We need to persist running request to prevent:
    // - False positive, i.e. thinking transaction is successful while it's actually not
    // - Committed, unreplied transaction
    // But we don't need to persist pending request?
    //
    // We don't persist pending requests for now to keep things simple for now
    storage ! RecordLocksStorage.UpdateStateRequest(from = self, state, runningRequest)
  }

  private def notifySubscribers(notify: Boolean)(nextState: => StateTransition[RequestMessage])(implicit stateData: RecordLocksStateData): StateTransition[RequestMessage] = {
    val transition = nextState

    if (notify && transition != StateTransition.stay) {
      notifySubscribers(stateData.subscribers, stateData.state, stateData.runningRequestOpt, stateData.pendingRequests)
    }

    transition
  }

  private def notifySubscribers(subscribers: Set[ActorRef], state: RecordLocksState, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest]): Unit =
    subscribers.foreach(_ ! StateChanged(state, runningRequest, pendingRequests))

}
