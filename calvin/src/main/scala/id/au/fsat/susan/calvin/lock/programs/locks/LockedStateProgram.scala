package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockedStateAlgebra
import id.au.fsat.susan.calvin.lock.programs.Program

object LockedStateProgram {
  private[programs] def locked(alg: LockedStateAlgebra[Id])(implicit s: CommonStates): Program[Id] = ???
}
