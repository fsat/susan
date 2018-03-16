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

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.cluster.ddata.ReplicatedData
import akka.stream.actor.ActorPublisherMessage.Cancel
import id.au.fsat.susan.calvin.{RecordId, RemoteMessage}

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
  case object Tick extends InternalMessage

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
  case class RecordLocksState(runningRequests: Seq[RunningRequest], pendingRequests: Seq[PendingRequest]) extends ReplicatedData {
    override type T = RecordLocksState
    override def merge(that: RecordLocksState): RecordLocksState = that
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

  override def receive: Receive = getState(RecordLocksState(Seq.empty, Seq.empty))(context.system.scheduler.schedule(0.millis, 100.millis, self, Tick))

  private def getState(state: RecordLocksState)(implicit scheduler: Cancellable): Receive = {
    case Tick =>

    case RecordLocksReplicatedStates.GetStateSuccess(stateReplicated) =>
      val stateUpdated = RecordLocksState(
        runningRequests = (state.runningRequests ++ stateReplicated.runningRequests).sortBy(_.createdAt),
        pendingRequests = (state.pendingRequests ++ stateReplicated.pendingRequests).sortBy(_.createdAt)
      )

      scheduler.cancel()

      context.become(manageLocks(stateUpdated)(context.system.scheduler.schedule(checkInterval, checkInterval, self, Tick)))

    case v: RecordLocksReplicatedStates.GetStateFailure =>

    case v: LockGetRequest =>
      // TODO: append to pending request

    case v: LockReturnRequest =>
    // TODO: we need pending request for this too?
  }

  private def manageLocks(state: RecordLocksState)(implicit scheduler: Cancellable): Receive = {
    case request @ LockGetRequest(_, _, timeoutObtain, _) if timeoutObtain > maxTimeoutObtain =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock obtain timeout of [${timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))

    case request @ LockGetRequest(_, _, _, timeoutReturn) if timeoutReturn > maxTimeoutReturn =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock return timeout of [${timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))

    case request @ LockGetRequest(requestId, recordId, _, timeoutReturn) =>
      val existingTransactions = state.runningRequests.filter(_.request.recordId == recordId)
      if (existingTransactions.isEmpty) {
        val now = Instant.now()
        val lock = Lock(requestId, recordId, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))
        val runningRequest = RunningRequest(sender(), request, now, lock)

        context.become(appendRunningRequest(runningRequest, state.copy(runningRequests = state.runningRequests :+ runningRequest)))

//        sender() ! LockGetSuccess(lock)
//
//        context.become(manageLocks(state.copy(runningRequests = state.runningRequests :+ runningRequest)))

      } else if (!state.pendingRequests.exists(_.request.requestId == requestId)) {
        val pendingRequest = PendingRequest(sender(), request, Instant.now())

        context.become(appendPendingRequest(pendingRequest, state.copy(pendingRequests = state.pendingRequests :+ pendingRequest)))

//        context.become(manageLocks(state.copy(pendingRequests = state.pendingRequests :+ pendingRequest)))
      }

    case LockReturnRequest(lock) =>
//      val now = Instant.now()
//      val isLate = now.isAfter(lock.returnDeadline)
//
//      if (state.runningRequests.exists(_.lock == lock)) {
//        val response = if (isLate) LockReturnLate(lock) else LockReturnSuccess(lock)
//        sender() ! response
//
//        context.become(manageLocks(state.copy(runningRequests = state.runningRequests.filterNot(_.lock == lock))))
//      } else {
//        sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
//      }
      context.become(returnLock(sender(), lock, state))

    case Tick =>
      val now = Instant.now()

      def canRemove(runningRequest: RunningRequest): Boolean = {
        val deadline = runningRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
        now.isAfter(deadline)
      }

      val runningTimedOut = state.runningRequests.filter(canRemove)
      val runningAlive = state.runningRequests.filterNot(canRemove)

//      runningTimedOut.foreach { v =>
//        v.caller ! LockExpired(v.lock)
//      }

      def timedOut(input: PendingRequest): Boolean = {
        val deadline = input.createdAt.plusNanos(input.request.timeoutObtain.toNanos)
        now.isAfter(deadline)
      }

      val pendingTimedOut = state.pendingRequests.filter(timedOut)
      val pendingAlive = state.pendingRequests.filterNot(timedOut)

//      pendingTimedOut.foreach { v =>
//        v.caller ! LockGetTimeout(v.request)
//      }

      def canProcess(input: PendingRequest): Boolean =
        !runningAlive.exists(_.request.recordId == input.request.recordId)

      val pendingAliveSorted = pendingAlive.sortBy(_.createdAt)
      val pendingAliveKept = pendingAliveSorted.take(maxPendingRequests)
      val pendingAliveDropped = pendingAliveSorted.takeRight(pendingAliveSorted.length - maxPendingRequests)

//      pendingAliveDropped.foreach { v =>
//        v.caller ! LockGetRequestDropped(v.request)
//      }

//      pendingAliveKept.find(canProcess) match {
//        case Some(v @ PendingRequest(caller, request, _)) =>
//          self.tell(request, sender = caller)
//          context.become(manageLocks(state.copy(runningRequests = runningAlive, pendingRequests = pendingAliveKept.filterNot(_ == v))))
//
//        case _ =>
//          context.become(manageLocks(state.copy(runningRequests = runningAlive, pendingRequests = pendingAliveKept)))
//      }

    context.become(processInternalState(state, runningAlive, runningTimedOut, pendingAliveKept, pendingTimedOut, pendingAliveDropped))
  }

  private def appendRunningRequest(runningRequest: RunningRequest, state: RecordLocksState)(implicit scheduler: Cancellable): Receive = ???
  private def appendPendingRequest(pendingRequest: PendingRequest, state: RecordLocksState)(implicit scheduler: Cancellable): Receive = ???
  private def returnLock(sender: ActorRef, lock: Lock, state: RecordLocksState)(implicit scheduler: Cancellable): Receive = ???
  private def processInternalState(state: RecordLocksState, runningAlive: Seq[RunningRequest], runningTimedOut: Seq[RunningRequest], pendingAlive: Seq[PendingRequest], pendingTimedOut: Seq[PendingRequest], pendingAliveDropped: Seq[PendingRequest])(implicit scheduler: Cancellable): Receive = ???
}
