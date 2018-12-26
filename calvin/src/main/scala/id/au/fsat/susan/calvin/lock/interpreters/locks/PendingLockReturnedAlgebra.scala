package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState._
import scala.language.higherKinds._

trait PendingLockReturnedAlgebra[F[_]] extends InitializedLockStateAlgebra {
  override type State = PendingLockReturnedState.type
  override val currentState = PendingLockReturnedState
}
