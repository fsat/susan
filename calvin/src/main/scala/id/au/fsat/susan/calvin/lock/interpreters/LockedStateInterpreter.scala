package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.Interpreters.filterExpired
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ LockedStateAlgo, Responses }
import id.au.fsat.susan.calvin.lock.storage.RecordLocksStorage

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
  removeStaleLockAfter: FiniteDuration,
  now: () => Instant = Interpreters.now) extends LockedStateAlgo[Id] {
  override type Interpreter = LockedStateInterpreter

  override def checkExpiry(): (Id[Responses], Either[LockedStateAlgo[Id], RecordLocksAlgo.PendingLockExpiredStateAlgo[Id]]) = {
    val currentTime = now()
    val deadline = lockedRequest.lock.returnDeadline.plusNanos(removeStaleLockAfter.toNanos)
    val isExpired = currentTime.isAfter(deadline)

    if (isExpired) {
      val next = PendingLockExpiredStateInterpreter(
        lockedRequest,
        self,
        recordLocksStorage,
        subscribers,
        pendingRequests,
        maxPendingRequests,
        maxTimeoutObtain,
        maxTimeoutReturn,
        removeStaleLockAfter,
        now)

      Id(Seq(
        recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, next.state, Some(lockedRequest)))) -> Right(next)
    } else
      Id(Seq.empty) -> Left(this)
  }

  override def lockReturn(): (Id[Responses], RecordLocksAlgo.PendingLockReturnedStateAlgo[Id]) = {
    Id(Seq(
      recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, PendingLockReturnedState, Some(lockedRequest)))) ->
      PendingLockReturnedStateInterpreter(
        lockedRequest,
        self,
        recordLocksStorage,
        subscribers,
        pendingRequests,
        maxPendingRequests,
        maxTimeoutObtain,
        maxTimeoutReturn,
        removeStaleLockAfter,
        now)
  }

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Id[Responses], LockedStateInterpreter) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else {
      Id(Seq.empty) -> copy(pendingRequests = pendingRequests :+ PendingRequest(sender, req, now()))
    }

  override def processPendingRequests(): (Id[Responses], LockedStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    Id(responses) -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Id[Responses], LockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    Id(response) -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Id[Responses], LockedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    Id(response) -> next
  }

  override def notifySubscribers(): Id[Responses] = {
    val message = StateChanged(state, None, pendingRequests)
    Id(subscribers.map(_ -> message).toSeq)
  }
}
