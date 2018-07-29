package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.Interpreters.filterExpired
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ LockedStateAlgo, Responses }

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

case class LockedStateInterpreter(
  lockedRequest: RecordLocks.RunningRequest,
  self: ActorRef,
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int,
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,
  now: () => Instant = Interpreters.now) extends LockedStateAlgo[Id] {
  override type Interpreter = LockedStateInterpreter

  override def isLockedRequestExpired: Boolean = ???

  override def checkExpiry(): (Responses, Either[LockedStateAlgo[Id], RecordLocksAlgo.PendingLockExpiredStateAlgo[Id]]) = ???

  override def lockReturnRequest(req: RecordLocks.LockReturnRequest, sender: ActorRef): (Responses, RecordLocksAlgo.PendingLockReturnedStateAlgo[Id]) = ???

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Responses, LockedStateInterpreter) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Seq(sender -> reply) -> this

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Seq(sender -> reply) -> this

    } else {
      Seq.empty -> copy(pendingRequests = pendingRequests :+ PendingRequest(sender, req, now()))
    }

  override def processPendingRequests(): (Responses, LockedStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    responses -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Responses, LockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    response -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Responses, LockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    response -> next
  }

  override def notifySubscribers(): Responses = {
    val message = StateChanged(state, None, pendingRequests)
    subscribers.map(_ -> message).toSeq
  }
}