package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.LockGetRequestFailure
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState.PendingLockedState
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RunningRequest
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages.UpdateStateRequest
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Response
import id.au.fsat.susan.calvin.lockdeprecate.RecordLocks.LockGetSuccess
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage.{UpdateStateFailure, UpdateStateSuccess}

trait PendingLockedStateAlgebra[F[_]] extends InitializedLockStateAlgebra {
  override type State = PendingLockedState.type
  override val currentState = PendingLockedState

  def runningRequest: RunningRequest

  def persistState(): (F[Request[UpdateStateRequest]], PendingLockedStateAlgebra[F])
  def success(message: Response[UpdateStateSuccess]): (F[Response[LockGetSuccess]], LockedStateAlgebra[F])
  def failure(message: Response[UpdateStateFailure]): (F[Response[LockGetRequestFailure]], IdleStateAlgebra[F])
}
