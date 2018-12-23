package id.au.fsat.susan.calvin.lock.interpreters.subscribers

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra._
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra._
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses
import id.au.fsat.susan.calvin.lock.messages.{ RemoteMessage, RequestMessage, ResponseMessage }

import scala.language.higherKinds

object SubscriberAlgebra {
  final case class SubscribeRequest(ref: ActorRef) extends RequestMessage with RemoteMessage
  final case class UnsubscribeRequest(ref: ActorRef) extends RequestMessage with RemoteMessage
  final case class UnsubscribeSuccess() extends ResponseMessage with RemoteMessage

  final case class StateChanged(
    currentState: RecordLocksState,
    previousState: Option[RecordLocksState],
    runningRequest: Option[RunningRequest],
    pendingRequests: Seq[PendingRequest]) extends ResponseMessage with RemoteMessage
}

trait SubscriberAlgebra[F[_]] {
  def previousState: RecordLocksState
  def currentState: RecordLocksState

  def subscribe(message: SubscribeRequest, runningRequest: Option[RunningRequest],
    pendingRequests: Seq[PendingRequest]): (F[Responses[StateChanged]], SubscriberAlgebra[F])
  def unsubscribe(message: SubscribeRequest): (F[Responses[UnsubscribeSuccess]], SubscriberAlgebra[F])
  def stateChange(nextState: RecordLocksState, runningRequest: Option[RunningRequest],
    pendingRequests: Seq[PendingRequest]): (F[Responses[StateChanged]], SubscriberAlgebra[F])
}
