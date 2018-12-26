package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.PendingLockExpiredAlgebra
import id.au.fsat.susan.calvin.lock.programs.Program

object PendingLockExpiredStateProgram {
  private[programs] def pendingLockExpired(alg: PendingLockExpiredAlgebra[Id])(implicit s: CommonStates): Program[Id] = ???
}
