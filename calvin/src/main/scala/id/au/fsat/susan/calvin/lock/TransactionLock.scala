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

import java.time.LocalDateTime
import java.util.UUID

import akka.actor.{ Actor, ActorLogging }
import id.au.fsat.susan.calvin.RecordId

import scala.concurrent.duration.FiniteDuration

object TransactionLock {

  /**
   * All messages to [[TransactionLock]] actor must extends this trait.
   */
  sealed trait Message

  /**
   * All messages indicating failure must extends this trait.
   */
  sealed trait FailureMessage extends Message

  /**
   * Id for a particular lock request.
   */
  case class RequestId(value: UUID)

  /**
   * The transaction lock for a particular record.
   */
  case class Lock(requestId: RequestId, recordId: RecordId, lockId: UUID, createdAt: LocalDateTime, returnDeadline: LocalDateTime)

  /**
   * Request to obtain a particular transaction lock.
   */
  case class LockGetRequest(requestId: RequestId, recordId: RecordId, timeoutObtain: FiniteDuration, timeoutReturn: FiniteDuration) extends Message

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
  override def receive: Receive = ???
}
