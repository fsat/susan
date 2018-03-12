package id.au.fsat.susan.calvin.lock

import akka.actor.ActorRef
import akka.cluster.ddata.{ ORMap, ORMapKey }
import id.au.fsat.susan.calvin.lock.RecordLocks.{ RecordLocksState, RequestId }

object RecordLocksStates {

  object DistributedData {
    val Key = ORMapKey[String, RecordLocksState]("record-locks")
    val InitialValue = ORMap.empty[String, RecordLocksState]

    def update(key: ActorRef, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???
    private def update(entityId: String, state: RecordLocksState): ORMap[String, RecordLocksState] => ORMap[String, RecordLocksState] = ???
  }

  sealed trait Message
  sealed trait RequestMessage extends Message
  sealed trait ResponseMessage extends Message
  sealed trait FailureMessage extends Exception with Message

  case class GetStateRequest(from: ActorRef) extends RequestMessage
  case class GetStateSuccess(state: RecordLocksState) extends ResponseMessage
  case class GetStateFailure(request: GetStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage

  case class UpdateStateRequest(from: ActorRef, state: RecordLocksState, requestId: RequestId) extends RequestMessage
  case class UpdateStateSuccess(state: RecordLocksState) extends ResponseMessage
  case class UpdateStateFailure(request: UpdateStateRequest, message: String, error: Option[Throwable]) extends FailureMessage with ResponseMessage
}

class RecordLocksStates {

}
