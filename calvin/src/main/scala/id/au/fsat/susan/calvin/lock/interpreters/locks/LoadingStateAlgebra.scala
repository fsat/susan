package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.PendingRequest
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState._
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Response

import scala.language.higherKinds

trait LoadingStateAlgebra[F[_]] extends LockStateAlgebra {
  type State = LoadingState.type
  override val currentState = LoadingState

  def handleRequest(request: Request[LockGetRequest]): (F[Either[Response[LockGetRequestInvalid], Response[LockGetRequestEnqueued]]], LoadingStateAlgebra[F])
  def load(): (F[Request[GetStateRequest]], LoadingStateAlgebra[F])
  def loaded(success: Response[GetStateSuccess], pendingRequests: Seq[PendingRequest]): InitializedLockStateAlgebra
  def loadedNoPriorState(pendingRequests: Seq[PendingRequest]): InitializedLockStateAlgebra
  def failed(failure: Response[GetStateFailure]): LoadingStateAlgebra[F]
}
