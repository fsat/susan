package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.{LockGetRequest, LockGetRequestEnqueued, LockGetRequestInvalid}
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState.IdleState
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Response

import scala.language.higherKinds

trait IdleStateAlgebra[F[_]] extends InitializedLockStateAlgebra {
  type State = IdleState.type
  override val currentState = IdleState

  type RespondWithInvalid = (F[Response[LockGetRequestInvalid]], IdleStateAlgebra[F])
  type RespondWithEnqueued = (F[Response[LockGetRequestEnqueued]], PendingLockedStateAlgebra[F])

  def handleRequest(request: Request[LockGetRequest]): Either[RespondWithInvalid, RespondWithEnqueued]
}
