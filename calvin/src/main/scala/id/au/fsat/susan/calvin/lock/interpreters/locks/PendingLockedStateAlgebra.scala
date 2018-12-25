package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.{ LockGetRequestFailure, LockGetRequestSuccess }
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState.PendingLockedState
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RunningRequest
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages.{ UpdateStateFailure, UpdateStateRequest, UpdateStateSuccess }
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Response

trait PendingLockedStateAlgebra[F[_]] extends InitializedLockStateAlgebra {
  override type State = PendingLockedState.type
  override val currentState = PendingLockedState

  def runningRequest: RunningRequest

  def persistState(): (F[Request[UpdateStateRequest]], PendingLockedStateAlgebra[F])
  def success(message: Response[UpdateStateSuccess]): (F[Response[LockGetRequestSuccess]], LockedStateAlgebra[F])
  def failure(message: Response[UpdateStateFailure]): (F[Response[LockGetRequestFailure]], IdleStateAlgebra[F])
}
