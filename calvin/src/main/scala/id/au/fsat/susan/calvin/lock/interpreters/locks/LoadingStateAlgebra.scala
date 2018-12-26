package id.au.fsat.susan.calvin.lock.interpreters.locks

import id.au.fsat.susan.calvin.lock.interpreters.locks.LoadingStateAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RecordLocksState._
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.RunningRequest
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra.PendingRequest
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.messages.RequestMessage
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object LoadingStateAlgebra {
  object Messages {
    sealed trait LoadedState extends RequestMessage
    sealed trait NotFound extends LoadedState
    case object NotFound extends NotFound

    sealed trait LoadedStateIdle extends LoadedState
    case object LoadedStateIdle extends LoadedStateIdle

    case class LoadedStateLocked(runningRequest: RunningRequest) extends LoadedState
    case class LoadedStatePendingLockExpired(runningRequest: RunningRequest) extends LoadedState
    case class LoadedStatePendingLockReturned(runningRequest: RunningRequest) extends LoadedState

    case class LoadFailure(message: String, error: Option[Throwable]) extends RequestMessage
  }
}

trait LoadingStateAlgebra[F[_]] extends LockStateAlgebra {
  type State = LoadingState.type
  override val currentState = LoadingState

  def load(): (F[Request[GetStateRequest]], LoadingStateAlgebra[F])
  def retry(failure: LoadFailure, delay: FiniteDuration, attempt: Int): (F[Request[GetStateRequest]], LoadingStateAlgebra[F])
  def loaded(success: Request[LoadedState], pendingRequests: Seq[PendingRequest]): InitializedLockStateAlgebra
  def failed(failure: Request[LoadFailure]): LoadingStateAlgebra[F]
}
