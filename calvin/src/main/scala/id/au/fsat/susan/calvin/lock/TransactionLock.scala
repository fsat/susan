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

import akka.actor.{ Actor, ActorLogging, Props }
import id.au.fsat.susan.calvin.RecordId

import scala.collection.immutable.{ Seq, Set }
import scala.concurrent.duration.FiniteDuration

object TransactionLock {

  def props(maxTimeoutObtain: FiniteDuration, maxTimeoutReturn: FiniteDuration): Props =
    Props(new TransactionLock(maxTimeoutObtain, maxTimeoutReturn))

  /**
   * All messages to [[TransactionLock]] actor must extends this trait.
   */
  sealed trait Message

  /**
   * All messages indicating failure must extends this trait.
   */
  sealed trait FailureMessage extends Exception with Message

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
}

/**
 * Responsible for transaction lock for a particular record.
 *
 * Before calling one or more operations to modify a certain record, the caller *MUST* call the [[TransactionLock]] to
 * obtain the lock associated to the entity to be modified.
 */
class TransactionLock(maxTimeoutObtain: FiniteDuration, maxTimeoutReturn: FiniteDuration) extends Actor with ActorLogging {
  import TransactionLock._

  override def receive: Receive = manageLocks(Seq.empty, Set.empty)

  private def manageLocks(locks: Seq[Lock], pendingRequests: Set[LockGetRequest]): Receive = {
    case request @ LockGetRequest(_, _, timeoutObtain, _) if timeoutObtain > maxTimeoutObtain =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock obtain timeout of [${timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))

    case request @ LockGetRequest(_, _, _, timeoutReturn) if timeoutReturn > maxTimeoutReturn =>
      sender() ! LockGetFailure(request, new IllegalArgumentException(s"The lock return timeout of [${timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))

    case request @ LockGetRequest(requestId, records, _, timeoutReturn) =>
      val existingTransactions = locks.filter(v => records.intersect(v.records).nonEmpty)
      if (existingTransactions.isEmpty) {
        val now = Instant.now()
        val lock = Lock(requestId, records, UUID.randomUUID(), createdAt = now, returnDeadline = now.plusNanos(timeoutReturn.toNanos))

        sender() ! LockGetSuccess(lock)

        context.become(manageLocks(locks :+ lock, pendingRequests))
      } else {
        context.become(manageLocks(locks, pendingRequests + request))
      }

    case LockReturnRequest(lock) =>
      val now = Instant.now()
      val isLate = now.isAfter(lock.returnDeadline)

      if (locks.contains(lock)) {
        val response = if (isLate) LockReturnLate(lock) else LockReturnSuccess(lock)
        sender() ! response

        context.become(manageLocks(locks.filter(_ == lock), pendingRequests))
      } else {
        sender() ! LockReturnFailure(lock, new IllegalArgumentException(s"The lock [$lock] is not registered"))
      }

    // TODO: need to process pending requests & remove stale locks - send internal heartbeat
  }
}
