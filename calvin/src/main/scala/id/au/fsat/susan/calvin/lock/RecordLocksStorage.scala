package id.au.fsat.susan.calvin.lock

import akka.actor.{ Actor, ActorRef, Props }
import akka.cluster.ddata.{ ORMap, ORMapKey }
import id.au.fsat.susan.calvin.lock.RecordLocks.{ PendingRequest, RecordLocksStateToPersist, RecordLocksState, RunningRequest }

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
  override def receive: Receive = ???
}