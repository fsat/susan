package id.au.fsat.susan.calvin.lock.interpreters.storage

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.{ RecordLocksState, RunningRequest }
import id.au.fsat.susan.calvin.lock.messages.{ RemoteMessage, RequestMessage, ResponseMessage }

import scala.language.higherKinds

object LockStorageAlgebra {
  object Messages {
    case class GetStateRequest(from: ActorRef) extends RequestMessage with RemoteMessage
    case class GetStateSuccess(state: RecordLocksState, runningRequest: Option[RunningRequest]) extends ResponseMessage with RemoteMessage
    sealed trait GetStateNotFound extends ResponseMessage with RemoteMessage
    case object GetStateNotFound extends GetStateNotFound
    case class GetStateFailure(request: GetStateRequest, message: String, error: Option[Throwable]) extends ResponseMessage with RemoteMessage

    case class UpdateStateRequest(from: ActorRef, state: RecordLocksState, runningRequest: Option[RunningRequest]) extends RequestMessage with RemoteMessage
    case class UpdateStateSuccess(state: RecordLocksState, runningRequest: Option[RunningRequest]) extends ResponseMessage with RemoteMessage
    case class UpdateStateFailure(request: UpdateStateRequest, message: String, error: Option[Throwable]) extends ResponseMessage with RemoteMessage
  }
}

trait LockStorageAlgebra[F[_]]
