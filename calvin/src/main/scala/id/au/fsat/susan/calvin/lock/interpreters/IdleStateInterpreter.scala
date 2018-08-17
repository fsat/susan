package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant
import java.util.UUID

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ IdleStateAlgo, PendingLockedStateAlgo, Responses }
import id.au.fsat.susan.calvin.lock.storage.RecordLocksStorage

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

case class IdleStateInterpreter(
  self: ActorRef,
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int,
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,
  removeStaleLockAfter: FiniteDuration,
  now: () => Instant = Interpreters.now) extends IdleStateAlgo[Id] {
  override type Interpreter = IdleStateInterpreter

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Id[Responses], Either[IdleStateAlgo[Id], PendingLockedStateAlgo[Id]]) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> Left(copy())

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> Left(copy())

    } else {
      val currentTime = now()
      val lock = Lock(req.requestId, req.recordId, UUID.randomUUID(), createdAt = currentTime, returnDeadline = currentTime.plusNanos(req.timeoutReturn.toNanos))
      val runningRequest = RunningRequest(sender, req, currentTime, lock)

      val responses = Seq(
        recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, LockedState, Some(runningRequest)))

      Id(responses) -> Right(PendingLockedStateInterpreter(
        lockedRequest = runningRequest,
        self,
        recordLocksStorage,
        subscribers,
        pendingRequests,
        maxPendingRequests,
        maxTimeoutObtain,
        maxTimeoutReturn,
        removeStaleLockAfter,
        now))
    }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Id[Responses], IdleStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    Id(response) -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Id[Responses], IdleStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    Id(response) -> next
  }

  override def notifySubscribers(): Id[Responses] = {
    val message = StateChanged(state, None, pendingRequests)
    Id(subscribers.map(_ -> message).toSeq)
  }

}
