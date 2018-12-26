package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState._
import scala.language.higherKinds._

trait PendingLockExpiredAlgebra[F[_]] extends InitializedLockStateAlgebra {
  override type State = PendingLockExpiredState.type
  override val currentState = PendingLockExpiredState
}
