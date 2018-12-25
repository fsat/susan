package id.au.fsat.susan.calvin.lock.interpreters.subscribers

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra._
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra.PendingRequest
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.{ Response, Responses }
import id.au.fsat.susan.calvin.lock.messages.{ RemoteMessage, RequestMessage, ResponseMessage }

import scala.language.higherKinds

object SubscriberAlgebra {
  object Messages {
    final case class SubscribeRequest(ref: ActorRef) extends RequestMessage with RemoteMessage
    final case class UnsubscribeRequest(ref: ActorRef) extends RequestMessage with RemoteMessage
    sealed trait UnsubscribeSuccess extends ResponseMessage with RemoteMessage
    case object UnsubscribeSuccess extends UnsubscribeSuccess

    final case class StateChanged(
      currentState: RecordLocksState,
      previousState: Option[RecordLocksState],
      runningRequest: Option[RunningRequest],
      pendingRequests: Seq[PendingRequest]) extends ResponseMessage with RemoteMessage
  }
}

trait SubscriberAlgebra[F[_]] {
  def previousState: RecordLocksState
  def currentState: RecordLocksState

  def subscribe(message: SubscribeRequest, runningRequest: Option[RunningRequest],
    pendingRequests: Seq[PendingRequest]): (F[Response[StateChanged]], SubscriberAlgebra[F])
  def unsubscribe(message: SubscribeRequest): (F[Response[UnsubscribeSuccess]], SubscriberAlgebra[F])
  def stateChange(nextState: RecordLocksState, runningRequest: Option[RunningRequest],
    pendingRequests: Seq[PendingRequest]): (F[Responses[StateChanged]], SubscriberAlgebra[F])
}
