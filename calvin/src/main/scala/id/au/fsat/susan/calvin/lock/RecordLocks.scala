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

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import id.au.fsat.susan.calvin.{ RecordId, RemoteMessage }

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object RecordLocks {
  val name = "transaction-locks"

  object StateTransition {
    object Stay extends StateTransition {
      override def pf: PartialFunction[RequestMessage, StateTransition] = {
        case _ => Stay
      }
      override def orElse(other: StateTransition): StateTransition = other
    }

    class PartialFunctionStateTransition(val pf: PartialFunction[RequestMessage, StateTransition]) extends StateTransition {
      override def orElse(other: StateTransition): StateTransition = new PartialFunctionStateTransition(pf.orElse(other.pf))
    }

    def apply(pf: PartialFunction[RequestMessage, StateTransition]): StateTransition = new PartialFunctionStateTransition(pf)
  }

  trait StateTransition {
    def pf: PartialFunction[RequestMessage, StateTransition]
    def orElse(other: StateTransition): StateTransition
  }

  def props()(implicit transactionLockSettings: RecordLockSettings): Props =
    Props(new RecordLocks())

  /**
   * All messages to [[RecordLocks]] actor must extends this trait.
   */
  sealed trait Message

  /**
   * All input messages to [[RecordLocks]] actor must extends this trait.
   */
  sealed trait RequestMessage extends RemoteMessage

  /**
   * All response messages from [[RecordLocks]] actor must extends this trait.
   */
  sealed trait ResponseMessage extends RemoteMessage

  /**
   * All self message must extends this trait.
   */
  sealed trait InternalMessage extends Message

  /**
   * All messages indicating failure must extends this trait.
   */
  sealed trait FailureMessage extends Exception with Message

  /**
   * Self message to perform the following things:
   * - processing pending lock requests
   * - removal of stale locks
   */
  case object Tick extends RequestMessage with InternalMessage

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
   * Represents a pending request for transaction lock
   */
  case class PendingRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant)

  /**
   * Represents a running request which has had transaction lock assigned.
   */
  case class RunningRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant, lock: Lock)

  /**
   * Represents the internal state of [[RecordLocks]] actor.
   */
  case class RecordLocksState(runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest])
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

  override def preStart(): Unit = {
    context.system.scheduler.schedule(checkInterval, checkInterval, self, Tick)
    // TODO: load state
  }

  override def receive: Receive = stateTransition(loadState(Seq.empty))

  private def stateTransition(currentState: StateTransition): Receive = {
    case v: RequestMessage =>
      val nextState = currentState.pf(v)
      context.become(stateTransition(if (nextState == StateTransition.Stay) currentState else nextState))
  }

  private def loadState(pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = loadState))
      .orElse(StateTransition {
        case Tick =>
          // TODO: wait for the loaded state prior deciding idle (or nextPendingRequest)
          if (pendingRequests.isEmpty)
            idle()
          else
            nextPendingRequest(pendingRequests)
      })

  private def idle(): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(StateTransition {
        case request @ LockGetRequest(requestId, recordId, _, timeoutReturn) =>
          val now = Instant.now()
          val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

          // TODO: persist lock in flight + no pending requests into state

          val runningRequest = RunningRequest(sender, request, Instant.now(), lock)
          pendingLockObtained(runningRequest, Seq.empty)

        case Tick =>
          // Nothing to do, everything clear
          StateTransition.Stay
      })

  private def pendingLockObtained(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = pendingLockObtained(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          pendingLockReturned(runningRequest, pendingRequests)

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.Stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out
          // TODO: clean up stale pending requests
          runningRequest.caller ! LockGetSuccess(runningRequest.lock)
          locked(runningRequest, pendingRequests)
      })

  private def locked(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = locked(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: persist lock returned + pending requests into state
          pendingLockReturned(runningRequest, pendingRequests)

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.Stay

        case Tick =>
          val now = Instant.now()
          val (pendingAliveKept, pendingAliveDropped) = filterExpired(now, pendingRequests)

          pendingAliveDropped.foreach { v =>
            v.caller ! LockGetRequestDropped(v.request)
          }

          if (isExpired(now, runningRequest))
            // TODO: persist lock expired + pending requests into state
            pendingLockExpired(runningRequest, pendingAliveKept)
          else
            locked(runningRequest, pendingAliveKept)
      })

  private def pendingLockExpired(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = pendingLockExpired(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is expired
          val lockExpired = LockExpired(runningRequest.lock)

          sender() ! lockExpired
          runningRequest.caller ! lockExpired

          if (pendingRequests.isEmpty)
            idle()
          else
            nextPendingRequest(pendingRequests)

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.Stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out
          // TODO: clean up stale pending requests

          runningRequest.caller ! LockExpired(runningRequest.lock)

          if (pendingRequests.isEmpty)
            idle()
          else
            nextPendingRequest(pendingRequests)
      })

  private def pendingLockReturned(runningRequest: RunningRequest, pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = pendingLockReturned(runningRequest, _)))
      .orElse(StateTransition {
        case LockReturnRequest(lock) if lock == runningRequest.lock =>
          // TODO: what should we do here? for now, we'll reply to both sender & running requests and lock is returned
          val lockReturned = LockReturnSuccess(runningRequest.lock)

          sender() ! lockReturned
          runningRequest.caller ! lockReturned

          if (pendingRequests.isEmpty)
            idle()
          else
            nextPendingRequest(pendingRequests)

        case LockReturnRequest(lock) =>
          sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
          StateTransition.Stay

        case Tick =>
          // TODO: wait for reply that state have been saved
          // TODO: cancel running requests if timed out
          // TODO: clean up stale pending requests

          val now = Instant.now()
          val isLate = now.isAfter(runningRequest.lock.returnDeadline)
          val reply = if (isLate) LockReturnLate(runningRequest.lock) else LockReturnSuccess(runningRequest.lock)

          runningRequest.caller ! reply

          nextPendingRequest(pendingRequests)
      })

  private def nextPendingRequest(pendingRequests: Seq[PendingRequest]): StateTransition =
    rejectInvalidLockGetRequest
      .orElse(rejectLockReturnRequest)
      .orElse(appendLockGetRequestToPending(pendingRequests)(nextState = nextPendingRequest))
      .orElse(StateTransition {
        case Tick =>
          if (pendingRequests.isEmpty)
            idle()
          else {
            val pendingRequest = pendingRequests.head

            val requestId = pendingRequest.request.requestId
            val recordId = pendingRequest.request.recordId
            val timeoutReturn = pendingRequest.request.timeoutReturn

            val now = Instant.now()
            val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

            // TODO: persist lock in flight + no pending requests into state

            val runningRequest = RunningRequest(pendingRequest.caller, pendingRequest.request, pendingRequest.createdAt, lock)
            pendingLockObtained(runningRequest, pendingRequests.tail)
          }
      })

  private def rejectInvalidLockGetRequest: StateTransition = StateTransition {
    case v @ LockGetRequest(_, _, timeoutObtain, _) if timeoutObtain > maxTimeoutObtain =>
      sender() ! LockGetFailure(v, new IllegalArgumentException(s"The lock obtain timeout of [${timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      StateTransition.Stay

    case v @ LockGetRequest(_, _, _, timeoutReturn) if timeoutReturn > maxTimeoutReturn =>
      sender() ! LockGetFailure(v, new IllegalArgumentException(s"The lock return timeout of [${timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      StateTransition.Stay
  }

  private def rejectLockReturnRequest: StateTransition = StateTransition {
    case LockReturnRequest(lock) =>
      sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
      StateTransition.Stay
  }

  private def appendLockGetRequestToPending(pendingRequests: Seq[PendingRequest])(nextState: Seq[PendingRequest] => StateTransition): StateTransition = StateTransition {
    case v: LockGetRequest =>
      val pendingRequest = PendingRequest(sender(), v, Instant.now())
      nextState(pendingRequests :+ pendingRequest)
  }

  private def isExpired(now: Instant, runningRequest: RunningRequest): Boolean = {
    val deadline = runningRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
    now.isAfter(deadline)
  }

  private def filterExpired(now: Instant, pendingRequests: Seq[PendingRequest]): (Seq[PendingRequest], Seq[PendingRequest]) = {
    def timedOut(input: PendingRequest): Boolean = {
      val deadline = input.createdAt.plusNanos(input.request.timeoutObtain.toNanos)
      now.isAfter(deadline)
    }

    val pendingTimedOut = pendingRequests.filter(timedOut)
    val pendingAlive = pendingRequests.filterNot(timedOut)

    pendingTimedOut.foreach { v =>
      v.caller ! LockGetTimeout(v.request)
    }

    val pendingAliveSorted = pendingAlive.sortBy(_.createdAt)
    val pendingAliveKept = pendingAliveSorted.take(maxPendingRequests)
    val pendingAliveDropped = pendingAliveSorted.takeRight(pendingAliveSorted.length - maxPendingRequests)

    pendingAliveKept -> pendingAliveDropped
  }
}
