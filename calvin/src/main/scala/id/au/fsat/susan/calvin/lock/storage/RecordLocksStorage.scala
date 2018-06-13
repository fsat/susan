package id.au.fsat.susan.calvin.lock.storage

import java.util.UUID

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.ReadAll
import akka.cluster.ddata._
import id.au.fsat.susan.calvin.{ RecordId, RemoteMessage, StateTransition }
import id.au.fsat.susan.calvin.lock.RecordLocks._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object RecordLocksStorage {
  val Name = "record-locks-storage"

  def props: Props = Props(new RecordLocksStorage)

  case class StoredValue(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ReplicatedData with RemoteMessage {
    override type T = StoredValue
    override def merge(that: StoredValue): StoredValue = that
  }

  val Key = ORMapKey[String, StoredValue]("record-locks")
  val InitialValue = ORMap.empty[String, StoredValue]

  def update(key: String, state: StoredValue): ORMap[String, StoredValue] => ORMap[String, StoredValue] = ???

  sealed trait Message
  sealed trait RequestMessage extends Message
  sealed trait ResponseMessage extends Message
  sealed trait FailureMessage extends Exception with Message

  case class GetStateRequest(from: ActorRef) extends RequestMessage
  case class GetStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ResponseMessage
  case object GetStateNotFound extends ResponseMessage
  case class GetStateFailure(request: GetStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class UpdateStateRequest(from: ActorRef, state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends RequestMessage
  case class UpdateStateSuccess(state: RecordLocksStateToPersist, runningRequest: Option[RunningRequest]) extends ResponseMessage
  case class UpdateStateFailure(request: UpdateStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class PendingGetRequest(id: UUID, message: GetStateRequest)

  case class ReplicatorMessageWrapper(message: Replicator.ReplicatorMessage) extends RequestMessage

  sealed trait StateData
  case class IdleStateData(pendingGetRequests: Seq[PendingGetRequest] = Seq.empty)
  case class UpdateStateData(
    runningUpdate: UpdateStateRequest,
    pendingUpdates: Map[RecordId, UpdateStateRequest],
    pendingGetRequests: Seq[PendingGetRequest] = Seq.empty)
}

class RecordLocksStorage extends Actor {
  import RecordLocksStorage._

  private implicit val node = Cluster(context.system)
  private val replicator = DistributedData(context.system).replicator

  override def receive: Receive = stateTransition(idle(IdleStateData()))

  private def stateTransition(currentState: StateTransition[RequestMessage]): Receive = {
    case v: RequestMessage =>
      val nextState = currentState.pf(v)
      context.become(stateTransition(if (nextState == StateTransition.stay) currentState else nextState))

    case v: Replicator.ReplicatorMessage =>
      self ! ReplicatorMessageWrapper(v)

    case Terminated(`replicator`) =>
      context.stop(self)
  }

  protected def recordIdFromRef(ref: ActorRef): String = ???

  private def idle(state: IdleStateData): StateTransition[RequestMessage] =
    handleGetRequests(state.pendingGetRequests)(v => idle(IdleStateData(v)))

  private def handleGetRequests(pendingGetRequests: Seq[PendingGetRequest])(nextState: Seq[PendingGetRequest] => StateTransition[RequestMessage]): StateTransition[RequestMessage] =
    StateTransition {
      case v: GetStateRequest =>
        val pending = PendingGetRequest(UUID.randomUUID(), v)
        val readConsistency = ReadAll(timeout = 5.seconds)
        replicator ! Replicator.Get(Key, readConsistency, Some(pending.id.toString))
        nextState(pendingGetRequests :+ pending)

      case ReplicatorMessageWrapper(v @ Replicator.GetSuccess(`Key`, Some(id: String))) =>
        val data = v.get(Key)
        pendingGetRequests
          .filter(_.id.toString == id)
          .map { req =>
            val from = req.message.from
            val recordId = recordIdFromRef(from)
            from -> data.get(recordId)
          }
          .foreach { v =>
            val (from, data) = v
            val reply = data.map(v => GetStateSuccess(v.state, v.runningRequest)).getOrElse(GetStateNotFound)
            from ! reply
          }

        nextState(pendingGetRequests.filterNot(_.id.toString == id))

      case ReplicatorMessageWrapper(Replicator.NotFound(`Key`, Some(id: String))) =>
        pendingGetRequests
          .filter(_.id.toString == id)
          .foreach(_.message.from ! GetStateNotFound)

        nextState(pendingGetRequests.filterNot(_.id.toString == id))

      case ReplicatorMessageWrapper(Replicator.GetFailure(`Key`, Some(id: String))) =>
        pendingGetRequests
          .filter(_.id.toString == id)
          .foreach(r => r.message.from ! GetStateFailure(r.message, "Unable to get record locks state", None))

        nextState(pendingGetRequests.filterNot(_.id.toString == id))
    }
}