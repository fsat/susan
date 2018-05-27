package id.au.fsat.susan.calvin.lock.storage

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import id.au.fsat.susan.calvin.StateTransition
import id.au.fsat.susan.calvin.lock.RecordLocks._

import scala.collection.immutable.Seq

object RecordLocksStorage {
  val Name = "record-locks-storage"

  def props: Props = Props(new RecordLocksStorage)

  object DistributedData {
    //    val Key = ORMapKey[String, RecordLocksStorage]("record-locks")
    //    val InitialValue = ORMap.empty[String, RecordLocksStorage]

    //    def update(key: ActorRef, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???
    //    private def update(entityId: String, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???
  }

  sealed trait Message
  sealed trait RequestMessage extends Message
  sealed trait ResponseMessage extends Message
  sealed trait FailureMessage extends Exception with Message

  case class GetStateRequest(from: ActorRef) extends RequestMessage
  case class GetStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest], pendingRequests: Seq[PendingRequest]) extends ResponseMessage
  case class GetStateFailure(request: GetStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class UpdateStateRequest(from: ActorRef, state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends RequestMessage
  case class UpdateStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ResponseMessage
  case class UpdateStateFailure(request: UpdateStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

}

class RecordLocksStorage extends Actor {
  import RecordLocksStorage._

  private val crdt = context.watch(createCrdt())

  override def receive: Receive = stateTransition(loading())

  protected def createCrdt(): ActorRef = ???

  private def stateTransition(currentState: StateTransition[RequestMessage]): Receive = {
    case v: RequestMessage =>
      val nextState = currentState.pf(v)
      context.become(stateTransition(if (nextState == StateTransition.stay) currentState else nextState))

    case Terminated(`crdt`) =>
      context.stop(self)
  }

  private def loading(): StateTransition[RequestMessage] = ???
  private def idle(): StateTransition[RequestMessage] = ???
  private def persisting(): StateTransition[RequestMessage] = ???
}