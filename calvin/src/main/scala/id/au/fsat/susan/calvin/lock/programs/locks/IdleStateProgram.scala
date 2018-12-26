package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.IdleStateAlgebra
import id.au.fsat.susan.calvin.lock.programs.Program

object IdleStateProgram {
  private[programs] def idle(alg: IdleStateAlgebra[Id])(implicit s: CommonStates): Program[Id] = ???
}
