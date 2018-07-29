package id.au.fsat.susan.calvin.lock.interpreters

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks.{ PendingRequest, StateChanged }
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ IdleStateAlgo, Responses }

import scala.collection.immutable.Seq

case class IdleStateInterpreter(
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int) extends IdleStateAlgo[Id] {
  override type Interpreter = IdleStateInterpreter

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Responses, RecordLocksAlgo.PendingLockedStateAlgo[Id]) = ???

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Responses, IdleStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    response -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Responses, IdleStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    response -> next
  }

  override def notifySubscribers(): Responses = {
    val message = StateChanged(state, None, pendingRequests)
    subscribers.map(_ -> message).toSeq
  }
}
