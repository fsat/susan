package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks.{LockGetRequestDropped, LockGetTimeout, PendingRequest, StateChanged}
import id.au.fsat.susan.calvin.lock.interpreters.Interpreters.filterExpired
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{PendingLockedStateAlgo, Responses}

import scala.collection.immutable.Seq

case class PendingLockedStateInterpreter(lockedRequest: RecordLocks.RunningRequest,
                                          recordLocksStorage: ActorRef,
                                          subscribers: Set[ActorRef] = Set.empty,
                                          pendingRequests: Seq[PendingRequest] = Seq.empty,
                                          maxPendingRequests: Int,
                                          now: () => Instant = Interpreters.now) extends PendingLockedStateAlgo[Id] {
  override type Interpreter = PendingLockedStateInterpreter

  override def isWaitingForAck(request: RecordLocks.RunningRequest): Boolean = lockedRequest == request

  override def markLocked(): (Responses, RecordLocksAlgo.LockedStateAlgo[Id]) = ???

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Responses, PendingLockedStateInterpreter) = ???

  override def processPendingRequests(): (Responses, PendingLockedStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    responses -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Responses, PendingLockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    response -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Responses, PendingLockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    response -> next
  }

  override def notifySubscribers(): Responses = {
    val message = StateChanged(state, None, pendingRequests)
    subscribers.map(_ -> message).toSeq
  }
}
