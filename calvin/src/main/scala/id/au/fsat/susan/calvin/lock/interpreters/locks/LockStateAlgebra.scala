package id.au.fsat.susan.calvin.lock.interpreters.locks

import java.io.Serializable
import java.time.Instant
import java.util.UUID

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState
import id.au.fsat.susan.calvin.lock.messages.RequestMessage

import scala.concurrent.duration.FiniteDuration

object LockStateAlgebra {
  trait RecordLocksState
  case object LoadingState extends RecordLocksState
  case object IdleState extends RecordLocksState
  case object PendingLockObtainedState extends RecordLocksState
  case object LockedState extends RecordLocksState
  case object PendingLockExpiredState extends RecordLocksState
  case object PendingLockReturnedState extends RecordLocksState
  case object NextPendingRequestState extends RecordLocksState

  final case class RecordId(value: Serializable) extends AnyVal
  final case class RequestId(value: UUID)
  final case class Lock(requestId: RequestId, recordId: RecordId, lockId: UUID, createdAt: Instant, returnDeadline: Instant)

  final case class LockGetRequest(requestId: RequestId, recordId: RecordId, timeoutObtain: FiniteDuration, timeoutReturn: FiniteDuration) extends RequestMessage

  final case class RunningRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant, lock: Lock)
  final case class PendingRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant)
}

trait LockStateAlgebra[A <: RecordLocksState] {
  def currentState: A
}
