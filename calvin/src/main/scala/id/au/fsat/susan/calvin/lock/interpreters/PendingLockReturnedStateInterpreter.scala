package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.Interpreters.filterExpired
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ PendingLockReturnedStateAlgo, Responses }

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

case class PendingLockReturnedStateInterpreter(
  returnedRequest: RecordLocks.RunningRequest,
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int,
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,

  now: () => Instant = Interpreters.now) extends PendingLockReturnedStateAlgo[Id] {
  override type Interpreter = PendingLockReturnedStateInterpreter

  override def isWaitingForAck(request: RecordLocks.RunningRequest): Boolean = returnedRequest == request

  override def lockReturnConfirmed(): (Responses, Either[RecordLocksAlgo.IdleStateAlgo[Id], RecordLocksAlgo.PendingLockedStateAlgo[Id]]) = ???

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Responses, PendingLockReturnedStateInterpreter) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Seq(sender -> reply) -> this

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Seq(sender -> reply) -> this

    } else {
      Seq.empty -> copy(pendingRequests = pendingRequests :+ PendingRequest(sender, req, Instant.now()))
    }

  override def processPendingRequests(): (Responses, PendingLockReturnedStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    responses -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Responses, PendingLockReturnedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    response -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Responses, PendingLockReturnedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    response -> next
  }

  override def notifySubscribers(): Responses = {
    val message = StateChanged(state, None, pendingRequests)
    subscribers.map(_ -> message).toSeq
  }
}
