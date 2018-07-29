package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant
import java.util.UUID

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo.{ IdleStateAlgo, Responses }
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

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Responses, Either[IdleStateAlgo[Id], RecordLocksAlgo.PendingLockedStateAlgo[Id]]) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Seq(sender -> reply) -> Left(copy())

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Seq(sender -> reply) -> Left(copy())

    } else {
      val currentTime = now()
      val lock = Lock(req.requestId, req.recordId, UUID.randomUUID(), createdAt = currentTime, returnDeadline = currentTime.plusNanos(req.timeoutReturn.toNanos))
      val runningRequest = RunningRequest(sender, req, currentTime, lock)

      Seq(
        recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, LockedState, Some(runningRequest))) -> Right(PendingLockedStateInterpreter(
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
