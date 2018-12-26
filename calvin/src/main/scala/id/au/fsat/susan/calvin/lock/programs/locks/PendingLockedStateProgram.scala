package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.PendingLockedStateAlgebra
import id.au.fsat.susan.calvin.lock.programs.Program

object PendingLockedStateProgram {
  private[programs] def pendingLocked(alg: PendingLockedStateAlgebra[Id])(implicit s: CommonStates): Program[Id] = ???
}
