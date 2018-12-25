package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState.LockedState

trait LockedStateAlgebra[F[_]] extends LockStateAlgebra {
  override type State = LockedState.type
  override val currentState = LockedState
}
