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
import id.au.fsat.susan.calvin.RecordId

import scala.collection.immutable.{ Seq, Set }
import scala.concurrent.duration._

object TransactionLocks {

  def props(maxTimeoutObtain: FiniteDuration, maxTimeoutReturn: FiniteDuration, removeStaleLockAfter: FiniteDuration, checkInterval: FiniteDuration): Props =
    Props(new TransactionLocks(maxTimeoutObtain, maxTimeoutReturn, removeStaleLockAfter, checkInterval))

  /**
   * All messages to [[TransactionLocks]] actor must extends this trait.
   */
  sealed trait Message

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
  case class Lock(requestId: RequestId, records: Seq[RecordId], lockId: UUID, createdAt: Instant, returnDeadline: Instant)

  /**
   * Request to obtain a particular transaction lock.
   */
  case class LockGetRequest(requestId: RequestId, records: Seq[RecordId], timeoutObtain: FiniteDuration, timeoutReturn: FiniteDuration) extends Message

  /**
   * The reply if the lock is successfully obtained.
   */
  case class LockGetSuccess(lock: Lock) extends Message

  /**
   * The reply if the lock can't be obtained within the timeout specified by [[LockGetRequest]].
   */
  case class LockGetTimeout(request: LockGetRequest) extends FailureMessage

  /**
   * The reply if there's an exception obtaining the lock.
   */
  case class LockGetFailure(request: LockGetRequest, cause: Throwable) extends FailureMessage

  /**
   * Sent to the caller which requests the lock when the lock has expired
   */
  case class LockExpired(lock: Lock) extends FailureMessage

  /**
   * Request to return a particular transaction lock.
   */
  case class LockReturnRequest(lock: Lock) extends Message

  /**
   * The reply if the lock is successfully returned.
   */
  case class LockReturnSuccess(lock: Lock) extends Message

  /**
   * The reply if the lock is returned past it's return deadline.
   */
  case class LockReturnLate(lock: Lock) extends FailureMessage

  /**
   * The reply if there's an exception obtaining the lock.
   */
  case class LockReturnFailure(lock: Lock, cause: Throwable) extends FailureMessage

  /**
   * Represents a pending request for transaction lock
   */
  case class PendingRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant)

  /**
   * Represents a running request which has had transaction lock assigned.
   */
  case class RunningRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant, lock: Lock)
}

/**
 * Responsible for transaction lock for a particular record.
 *
 * Before calling one or more operations to modify a certain record, the caller *MUST* call the [[TransactionLocks]] to
 * obtain the lock associated to the entity to be modified.
 *
 * @param maxTimeoutObtain the maximum duration allowable waiting for a lock to be available
 * @param maxTimeoutReturn the maximum duration allowable to return a lock.
 *                         The lock which is not returned past this duration will be considered stale.
 * @param removeStaleLockAfter the duration where stale locks will be removed.
 * @param checkInterval the internal polling period to for removal of stale locks and processing of pending requests
 */
class TransactionLocks(maxTimeoutObtain: FiniteDuration, maxTimeoutReturn: FiniteDuration, removeStaleLockAfter: FiniteDuration, checkInterval: FiniteDuration) extends Actor with ActorLogging {
  import TransactionLocks._

  import context.dispatcher

  override def preStart(): Unit = {
    context.system.scheduler.schedule(checkInterval, checkInterval, self, Tick)
  }

  override def receive: Receive = manageLocks(Seq.empty, Seq.empty)

  private def manageLocks(runningRequest: Seq[RunningRequest], pendingRequests: Seq[PendingRequest]): Receive = {
    case request @ LockGetRequest(_, _, timeoutObtain, _) if timeoutObtain > maxTimeoutObtain =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock obtain timeout of [${timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))

    case request @ LockGetRequest(_, _, _, timeoutReturn) if timeoutReturn > maxTimeoutReturn =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock return timeout of [${timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))

    case request @ LockGetRequest(requestId, records, _, timeoutReturn) =>
      val existingTransactions = runningRequest.filter(v => records.intersect(v.request.records).nonEmpty)
      if (existingTransactions.isEmpty) {
        val now = Instant.now()
        val lock = Lock(requestId, records, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

        sender() ! LockGetSuccess(lock)

        context.become(manageLocks(runningRequest :+ RunningRequest(sender(), request, now, lock), pendingRequests))
      } else {
        context.become(manageLocks(runningRequest, pendingRequests :+ PendingRequest(sender(), request, Instant.now())))
      }

    case LockReturnRequest(lock) =>
      val now = Instant.now()
      val isLate = now.isAfter(lock.returnDeadline)

      if (runningRequest.map(_.lock).contains(lock)) {
        val response = if (isLate) LockReturnLate(lock) else LockReturnSuccess(lock)
        sender() ! response

        context.become(manageLocks(runningRequest.filterNot(_.lock == lock), pendingRequests))
      } else {
        sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
      }

    case Tick =>
      val now = Instant.now()

      def canRemove(runningRequest: RunningRequest): Boolean = {
        val deadline = runningRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
        now.isAfter(deadline)
      }

      val (runningTimedOut, runningAlive) = runningRequest.span(canRemove)

      runningTimedOut.foreach { v =>
        v.caller ! LockExpired(v.lock)
      }

      def timedOut(input: PendingRequest): Boolean = {
        val deadline = input.createdAt.plusNanos(input.request.timeoutObtain.toNanos)
        now.isAfter(deadline)
      }

      val (pendingTimedOut, pendingAlive) = pendingRequests.span(timedOut)

      pendingTimedOut.foreach { v =>
        v.caller ! LockGetTimeout(v.request)
      }

      def canProcess(input: PendingRequest): Boolean =
        runningAlive.flatMap(_.request.records).intersect(input.request.records).isEmpty

      pendingAlive.find(canProcess) match {
        case Some(v @ PendingRequest(caller, request, _)) =>
          self.tell(request, sender = caller)
          context.become(manageLocks(runningAlive, pendingAlive.filterNot(_ == v)))

        case _ =>
          context.become(manageLocks(runningAlive, pendingAlive))
      }

    // TODO: need to cap the maximum number of transaction locks
  }
}
