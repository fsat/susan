package id.au.fsat.susan.calvin.lock.storage

import java.util.UUID

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import id.au.fsat.susan.calvin.StateTransition
import id.au.fsat.susan.calvin.lock.RecordLocks._

import scala.collection.immutable.Seq

object RecordLocksStorage {
  val Name = "record-locks-storage"

  def props: Props = Props(new RecordLocksStorage)

  //    val Key = ORMapKey[String, RecordLocksStorage]("record-locks")
  //    val InitialValue = ORMap.empty[String, RecordLocksStorage]

  //    def update(key: ActorRef, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???
  //    private def update(entityId: String, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???

  sealed trait Message
  sealed trait RequestMessage extends Message
  sealed trait ResponseMessage extends Message
  sealed trait FailureMessage extends Exception with Message

  case class GetStateRequest(from: ActorRef) extends RequestMessage
  case class GetStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ResponseMessage
  case class GetStateFailure(request: GetStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class UpdateStateRequest(from: ActorRef, state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends RequestMessage
  case class UpdateStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ResponseMessage
  case class UpdateStateFailure(request: UpdateStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class PendingRequest(id: UUID, message: RequestMessage)
}

class RecordLocksStorage extends Actor {
  import RecordLocksStorage._

  private implicit val node = Cluster(context.system)
  private val replicator = DistributedData(context.system).replicator

  override def receive: Receive = stateTransition(all())

  private def stateTransition(currentState: StateTransition[RequestMessage]): Receive = {
    case v: RequestMessage =>
      val nextState = currentState.pf(v)
      context.become(stateTransition(if (nextState == StateTransition.stay) currentState else nextState))

    case Terminated(`replicator`) =>
      context.stop(self)
  }

  private def all(): StateTransition[RequestMessage] = all(Seq.empty)
  private def all(pendingRequest: Seq[PendingRequest]): StateTransition[RequestMessage] = ???
}