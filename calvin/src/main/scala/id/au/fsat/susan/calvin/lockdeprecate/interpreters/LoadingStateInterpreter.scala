package id.au.fsat.susan.calvin.lockdeprecate.interpreters

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lockdeprecate.RecordLocks
import id.au.fsat.susan.calvin.lockdeprecate.RecordLocks._
import id.au.fsat.susan.calvin.lockdeprecate.interpreters.RecordLocksAlgo.{ LoadingStateAlgo, Responses }
import id.au.fsat.susan.calvin.lockdeprecate.interpreters.Interpreters.{ filterExpired, EmptyResponse }
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

case class LoadingStateInterpreter(
  self: ActorRef,
  recordLocksStorage: ActorRef,
  subscribers: Set[ActorRef] = Set.empty,
  pendingRequests: Seq[PendingRequest] = Seq.empty,
  maxPendingRequests: Int,
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,
  removeStaleLockAfter: FiniteDuration,
  now: () => Instant = Interpreters.now) extends LoadingStateAlgo[Id] {
  override type Interpreter = LoadingStateInterpreter

  override def load(): (Id[Responses], LoadingStateAlgo[Id]) =
    Id(Seq(
      recordLocksStorage -> RecordLocksStorage.GetStateRequest(self))) -> this

  override def loaded(state: RecordLocks.RecordLocksState, runningRequest: Option[RecordLocks.RunningRequest]): (Id[Responses], RecordLocksAlgo[Id]) = {
    def idleOrNextPending(persistIdleState: Boolean): (Id[Responses], RecordLocksAlgo[Id]) = {
      if (pendingRequests.isEmpty) {
        val responses = if (persistIdleState) {
          Seq(recordLocksStorage -> RecordLocksStorage.UpdateStateRequest(from = self, IdleState, None))
        } else
          EmptyResponse

        //        Id(responses) -> IdleStateInterpreter(
        //          self,
        //          recordLocksStorage,
        //          subscribers,
        //          pendingRequests,
        //          maxPendingRequests,
        //          maxTimeoutObtain,
        //          maxTimeoutReturn,
        //          removeStaleLockAfter,
        //          now)
        ???
      } else {
        //        val interpreter = IdleStateInterpreter(
        //          self,
        //          recordLocksStorage,
        //          subscribers,
        //          pendingRequests.tail,
        //          maxPendingRequests,
        //          maxTimeoutObtain,
        //          maxTimeoutReturn,
        //          removeStaleLockAfter,
        //          now)
        //
        //        val (interpreterResponses, next) = interpreter.lockRequest(pendingRequests.head.request, pendingRequests.head.caller)
        //        val subscriberResponses = interpreter.notifySubscribers()
        //        val responses =
        //          for {
        //            r <- interpreterResponses
        //            s <- subscriberResponses
        //          } yield r ++ s
        //
        //        responses -> next.merge
        ???
      }
    }

    def withRunningRequest(f: RunningRequest => (Id[Responses], RecordLocksAlgo[Id])): (Id[Responses], RecordLocksAlgo[Id]) =
      runningRequest match {
        case Some(r) => f(r)
        case None    => idleOrNextPending(persistIdleState = true)
      }

    state match {
      case IdleState => idleOrNextPending(persistIdleState = false)

      case LockedState =>
        withRunningRequest { req =>
          Id(Seq.empty) -> LockedStateInterpreter(
            req,
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

      case PendingLockExpiredState =>
        withRunningRequest { req =>
          val pendingResponses = Seq(req.caller -> LockExpired(req.lock)) ++
            subscribers.map(_ -> StateChanged(PendingLockExpiredState, Some(req), pendingRequests))

          val (r, next) = idleOrNextPending(persistIdleState = true)
          r.map(v => pendingResponses ++ v) -> next
        }

      case PendingLockReturnedState =>
        withRunningRequest { req =>
          val isLate = now().isAfter(req.lock.returnDeadline)
          val lockReturnedResponse = if (isLate) LockReturnLate(req.lock) else LockReturnSuccess(req.lock)

          val pendingResponses = Seq(req.caller -> lockReturnedResponse) ++
            subscribers.map(_ -> StateChanged(PendingLockReturnedState, Some(req), pendingRequests))

          val (r, next) = idleOrNextPending(persistIdleState = true)
          r.map(v => pendingResponses ++ v) -> next
        }

    }
  }

  override def loadFailure(message: String, error: Option[Throwable]): (Id[Responses], LoadingStateAlgo[Id]) =
    Id(Seq(
      recordLocksStorage -> RecordLocksStorage.GetStateRequest(self))) -> this

  override def lockRequest(req: RecordLocks.LockGetRequest, sender: ActorRef): (Id[Responses], LoadingStateInterpreter) =
    if (req.timeoutObtain > maxTimeoutObtain) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock obtain timeout of [${req.timeoutObtain.toMillis} ms] is larger than allowable [${maxTimeoutObtain.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else if (req.timeoutReturn > maxTimeoutReturn) {
      val reply = LockGetFailure(req, new IllegalArgumentException(s"The lock return timeout of [${req.timeoutReturn.toMillis} ms] is larger than allowable [${maxTimeoutReturn.toMillis} ms]"))
      Id(Seq(sender -> reply)) -> this

    } else {
      Id(Seq.empty) -> copy(pendingRequests = pendingRequests :+ PendingRequest(sender, req, now()))
    }

  override def processPendingRequests(): (Id[Responses], LoadingStateInterpreter) = {
    val (pendingTimedOut, pendingAliveKept, pendingAliveDropped) = filterExpired(now(), pendingRequests, maxPendingRequests)

    val responses: Responses =
      pendingTimedOut.map(v => v.caller -> LockGetTimeout(v.request)) ++
        pendingAliveDropped.map(v => v.caller -> LockGetRequestDropped(v.request))

    val next = copy(pendingRequests = pendingAliveKept)

    Id(responses) -> next
  }

  override def subscribe(req: RecordLocks.SubscribeRequest, sender: ActorRef): (Id[Responses], LoadingStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.SubscribeSuccess)
    val next = copy(subscribers = subscribers + req.ref)
    Id(response) -> next
  }

  override def unsubscribe(req: RecordLocks.UnsubscribeRequest, sender: ActorRef): (Id[Responses], LoadingStateInterpreter) = {
    val response = Seq(sender -> RecordLocks.UnsubscribeSuccess)
    val next = copy(subscribers = subscribers.filterNot(_ == req.ref))
    Id(response) -> next
  }

  override def notifySubscribers(): Id[Responses] =
    Id(subscribers.map(v => v -> StateChanged(state, None, pendingRequests)).toSeq)
}