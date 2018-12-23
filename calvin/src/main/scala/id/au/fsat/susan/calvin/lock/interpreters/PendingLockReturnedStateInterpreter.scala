package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant
import java.util.UUID

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.Interpreters.filterExpired
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ PendingLockReturnedStateAlgo, Responses }
import id.au.fsat.susan.calvin.lock.storage.RecordLocksStorage

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

case class PendingLockReturnedStateInterpreter(
  returnedRequest: RecordLocks.RunningRequest,
  self: ActorRef,
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int,
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,
  removeStaleLockAfter: FiniteDuration,
  now: () => Instant = Interpreters.now,
  generateLockId: () => UUID = UUID.randomUUID) extends PendingLockReturnedStateAlgo[Id] {
  override type Interpreter = PendingLockReturnedStateInterpreter

  override def lockReturnConfirmed(): (Id[Responses], Either[RecordLocksAlgo.IdleStateAlgo[Id], RecordLocksAlgo.PendingLockedStateAlgo[Id]]) = {
    val isLate = now().isAfter(returnedRequest.lock.returnDeadline)
    val reply = if (isLate) LockReturnLate(returnedRequest.lock) else LockReturnSuccess(returnedRequest.lock)

    val callerReply = Seq(returnedRequest.caller -> reply)

    pendingRequests.headOption match {
      case Some(v) =>
        val currentTime = now()
        val req = v.request
        val sender = v.caller
        val lock = Lock(req.requestId, req.recordId, generateLockId(), createdAt = currentTime, returnDeadline = currentTime.plusNanos(req.timeoutReturn.toNanos))
        val runningRequest = RunningRequest(sender, req, currentTime, lock)

        val persistStateReply = Seq(
          recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, LockedState, Some(runningRequest)))

        Id(callerReply ++ persistStateReply) -> Right(PendingLockedStateInterpreter(
          lockedRequest = runningRequest,
          self,
          recordLocksStorage,
          subscribers,
          pendingRequests.tail,
          maxPendingRequests,
          maxTimeoutObtain,
          maxTimeoutReturn,
          removeStaleLockAfter,
          now))

      case None =>
        val persistStateReply = Seq(
          recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, IdleState, None))

        Id(persistStateReply) -> Left(IdleStateInterpreter(
          self,
          recordLocksStorage,
          subscribers,
          maxPendingRequests,
          maxTimeoutObtain,
          maxTimeoutReturn,
          removeStaleLockAfter,
          now,
          generateLockId))
    }
  }

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Id[Responses], PendingLockReturnedStateInterpreter) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else {
      Id(Seq.empty) -> copy(pendingRequests = pendingRequests :+ PendingRequest(sender, req, now()))
    }

  override def processPendingRequests(): (Id[Responses], PendingLockReturnedStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    Id(responses) -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Id[Responses], PendingLockReturnedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    Id(response) -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Id[Responses], PendingLockReturnedStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    Id(response) -> next
  }

  override def notifySubscribers(): Id[Responses] = {
    val message = StateChanged(state, None, pendingRequests)
    Id(subscribers.map(_ -> message).toSeq)
  }
}
