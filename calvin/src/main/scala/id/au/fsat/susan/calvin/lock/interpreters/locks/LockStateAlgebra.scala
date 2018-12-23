package id.au.fsat.susan.calvin.lock.interpreters.locks

import java.io.Serializable
import java.time.Instant
import java.util.UUID

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.LockGetRequest
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.{ InitializedRecordLockState, RecordLocksState }
import id.au.fsat.susan.calvin.lock.messages.{ RemoteMessage, RequestMessage, ResponseMessage }

import scala.concurrent.duration.FiniteDuration

object LockStateAlgebra {
  object RecordLocksState {
    sealed trait LoadingState extends RecordLocksState
    case object LoadingState extends RecordLocksState

    sealed trait IdleState extends InitializedRecordLockState
    case object IdleState extends IdleState

    sealed trait PendingLockObtainedState extends InitializedRecordLockState
    case object PendingLockObtainedState extends PendingLockObtainedState

    sealed trait LockedState extends InitializedRecordLockState
    case object LockedState extends LockedState

    sealed trait PendingLockExpiredState extends InitializedRecordLockState
    case object PendingLockExpiredState extends PendingLockExpiredState

    sealed trait PendingLockReturnedState extends InitializedRecordLockState
    case object PendingLockReturnedState extends PendingLockReturnedState

    sealed trait NextPendingRequestState extends InitializedRecordLockState
    case object NextPendingRequestState extends NextPendingRequestState
  }
  sealed trait RecordLocksState
  sealed trait InitializedRecordLockState extends RecordLocksState

  final case class RecordId(value: Serializable) extends AnyVal
  final case class RequestId(value: UUID)
  final case class Lock(requestId: RequestId, recordId: RecordId, lockId: UUID, createdAt: Instant, returnDeadline: Instant)
  final case class RunningRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant, lock: Lock)

  object Messages {
    final case class LockGetRequest(requestId: RequestId, recordId: RecordId, timeoutObtain: FiniteDuration, timeoutReturn: FiniteDuration) extends RequestMessage
    sealed trait LockGetResponse extends ResponseMessage with RemoteMessage
    case class LockGetRequestInvalid() extends LockGetResponse
    case class LockGetRequestEnqueued() extends LockGetResponse
    case class LockGetRequestSuccess() extends LockGetResponse
    case class LockGetRequestDropped() extends LockGetResponse
  }
}

trait LockStateAlgebra {
  type State <: RecordLocksState
  def currentState: State
}

trait InitializedLockStateAlgebra extends LockStateAlgebra {
  override type State <: InitializedRecordLockState
}