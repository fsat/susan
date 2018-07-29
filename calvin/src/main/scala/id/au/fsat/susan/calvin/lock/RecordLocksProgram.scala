package id.au.fsat.susan.calvin.lock

import akka.actor.{ Actor, ActorLogging, ActorRef, Terminated }
import id.au.fsat.susan.calvin.{ Id }
import id.au.fsat.susan.calvin.lock.RecordLocks._
import id.au.fsat.susan.calvin.lock.interpreters.RecordLocksAlgo._
import id.au.fsat.susan.calvin.lock.storage.RecordLocksStorage

class RecordLocksProgram(interpreter: LoadingStateAlgo[Id]) extends Actor with ActorLogging {

  private val storage = context.watch(createStorage())

  override def receive: Receive = {
    val (responses, next) = interpreter.load()
    responses.foreach(send)
    loading(next)
  }

  private def loading(interpreter: LoadingStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      next.notifySubscribers().foreach(send)
      loading(next)

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      loading(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      loading(next)

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateSuccess(state, runningRequestLoaded)) =>
      val (responses, next) = interpreter.loaded(state, runningRequestLoaded)
      responses.foreach(send)

      next match {
        case v: LoadingStateAlgo[Id]             => loading(v)
        case v: IdleStateAlgo[Id]                => idle(v)
        case v: PendingLockedStateAlgo[Id]       => pendingLocked(v)
        case v: LockedStateAlgo[Id]              => locked(v)
        case v: PendingLockReturnedStateAlgo[Id] => pendingLockReturned(v)
        case v: PendingLockExpiredStateAlgo[Id]  => pendingLockExpired(v)
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateFailure(_, message, error)) =>
      // TODO: exponential backoff
      val (responses, next) = interpreter.loadFailure(message, error)
      responses.foreach(send)
      loading(next)

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      loading(interpreter)

    case _: LockReturnRequest =>
      // Ignore
      loading(interpreter)

    case LockExpiryCheck =>
      // Ignore
      loading(interpreter)

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      responses.foreach(send)
      loading(next)

  }

  private def idle(interpreter: IdleStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      next match {
        case Left(v) => idle(v)

        case Right(v) =>
          v.notifySubscribers().foreach(send)
          pendingLocked(v)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      idle(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      idle(next)

    case _: RecordLocksStorageMessageWrapper =>
      // Ignore
      idle(interpreter)

    case _: LockReturnRequest =>
      // Ignore
      idle(interpreter)

    case LockExpiryCheck =>
      // Ignore
      idle(interpreter)

    case ProcessPendingRequests =>
      // Ignore
      idle(interpreter)

  }

  private def pendingLocked(interpreter: PendingLockedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      pendingLocked(next)

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      pendingLocked(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      pendingLocked(next)

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest))) if interpreter.lockedRequest == runningRequest =>
      val (responses, next) = interpreter.markLocked()
      responses.foreach(send)
      next.notifySubscribers().foreach(send)
      locked(next)

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      pendingLocked(interpreter)

    case _: LockReturnRequest =>
      // Ignore
      pendingLocked(interpreter)

    case LockExpiryCheck =>
      // Ignore
      pendingLocked(interpreter)

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      responses.foreach(send)
      pendingLocked(next)
  }

  private def locked(interpreter: LockedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      locked(next)

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      locked(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      locked(next)

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      locked(interpreter)

    case v: LockReturnRequest if v.lock == interpreter.lockedRequest.lock =>
      val (responses, next) = interpreter.lockReturn()
      responses.foreach(send)
      next.notifySubscribers().foreach(send)

      pendingLockReturned(next)

    case v: LockReturnRequest =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      locked(interpreter)

    case LockExpiryCheck =>
      val (responses, next) = interpreter.checkExpiry()
      responses.foreach(send)
      next match {
        case Left(unexpired) =>
          locked(unexpired)

        case Right(expired) =>
          expired.notifySubscribers().foreach(send)
          pendingLockExpired(expired)
      }

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      responses.foreach(send)
      locked(next)
  }

  private def pendingLockReturned(interpreter: PendingLockReturnedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      pendingLockReturned(next)

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      pendingLockReturned(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      pendingLockReturned(next)

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest))) if interpreter.returnedRequest == runningRequest =>
      val (responses, next) = interpreter.lockReturnConfirmed()
      responses.foreach(send)
      next match {
        case Left(v) =>
          v.notifySubscribers().foreach(send)
          idle(v)

        case Right(v) =>
          v.notifySubscribers().foreach(send)
          pendingLocked(v)
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      pendingLockReturned(interpreter)

    case _: LockReturnRequest =>
      // Ignore
      pendingLockReturned(interpreter)

    case LockExpiryCheck =>
      // Ignore
      pendingLockReturned(interpreter)

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      responses.foreach(send)
      pendingLockReturned(next)
  }

  private def pendingLockExpired(interpreter: PendingLockExpiredStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      responses.foreach(send)
      pendingLockExpired(next)

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      responses.foreach(send)
      pendingLockExpired(next)

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      responses.foreach(send)
      pendingLockExpired(next)

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(PendingLockExpiredState, Some(runningRequest))) if interpreter.expiredRequest == runningRequest =>
      val (responses, next) = interpreter.lockExpiryConfirmed()
      responses.foreach(send)
      next match {
        case Left(v) =>
          v.notifySubscribers().foreach(send)
          idle(v)

        case Right(v) =>
          v.notifySubscribers().foreach(send)
          pendingLocked(v)
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      pendingLockExpired(interpreter)

    case v: LockReturnRequest if interpreter.expiredRequest.lock == v.lock =>
      val (responses, next) = interpreter.lockReturnedLate(v, sender())
      responses.foreach(send)
      pendingLockExpired(next)

    case v: LockReturnRequest =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      pendingLockExpired(interpreter)

    case LockExpiryCheck =>
      // Ignore
      pendingLockExpired(interpreter)

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      responses.foreach(send)
      pendingLockExpired(next)
  }

  private def send(input: (ActorRef, _)): Unit =
    input._1 ! input._2

  private def onRequest(handler: RequestMessage => Receive): Receive = {
    case v: RequestMessage => context.become(handler(v))

    case v: RecordLocksStorage.Message =>
      val wrapper = RecordLocksStorageMessageWrapper(v)
      self ! wrapper

    case Terminated(`storage`) =>
      context.stop(self)
  }

  private def createStorage(): ActorRef = ???
}
