package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState.IdleState

import scala.language.higherKinds

trait IdleStateAlgebra[F[_]] extends InitializedLockStateAlgebra {
  type State = IdleState.type
  override val currentState = IdleState
}
