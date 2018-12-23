package id.au.fsat.susan.calvin.lockdeprecate

import akka.actor.{ Actor, ActorLogging, ActorRef, Terminated }
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lockdeprecate.RecordLocks._
import id.au.fsat.susan.calvin.lockdeprecate.interpreters.Interpreters
import id.au.fsat.susan.calvin.lockdeprecate.interpreters.RecordLocksAlgo._
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage

class RecordLocksProgram(interpreter: LoadingStateAlgo[Id]) extends Actor with ActorLogging {

  private val storage = context.watch(createStorage())

  override def receive: Receive = {
    val (responses, next) = interpreter.load()
    responses.map(_.foreach(send))
    loading(next)
  }

  private def loading(interpreter: LoadingStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
        s <- next.notifySubscribers()
      } yield {
        (r ++ s).foreach(send)
        loading(next)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        loading(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        loading(next)
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateSuccess(state, runningRequestLoaded)) =>
      val (responses, next) = interpreter.loaded(state, runningRequestLoaded)
      for {
        r <- responses
      } yield {
        r.foreach(send)
        next match {
          case v: LoadingStateAlgo[Id]             => loading(v)
          case v: IdleStateAlgo[Id]                => idle(v)
          case v: PendingLockedStateAlgo[Id]       => pendingLocked(v)
          case v: LockedStateAlgo[Id]              => locked(v)
          case v: PendingLockReturnedStateAlgo[Id] => pendingLockReturned(v)
          case v: PendingLockExpiredStateAlgo[Id]  => pendingLockExpired(v)
        }
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.GetStateFailure(_, message, error)) =>
      // TODO: exponential backoff
      val (responses, next) = interpreter.loadFailure(message, error)
      for {
        r <- responses
      } yield {
        r.foreach(send)
        loading(next)
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      Id(loading(interpreter))

    case _: LockReturnRequest =>
      // Ignore
      Id(loading(interpreter))

    case LockExpiryCheck =>
      // Ignore
      Id(loading(interpreter))

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      for {
        r <- responses
      } yield {
        r.foreach(send)
        loading(next)
      }
  }

  private def idle(interpreter: IdleStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
        s <- next match {
          case Left(_)  => Id(Interpreters.EmptyResponse)
          case Right(v) => v.notifySubscribers()
        }
      } yield {
        (r ++ s).foreach(send)
        next match {
          case Left(k)  => idle(k)
          case Right(k) => pendingLocked(k)
        }
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        idle(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        idle(next)
      }

    case _: RecordLocksStorageMessageWrapper =>
      // Ignore
      Id(idle(interpreter))

    case _: LockReturnRequest =>
      // Ignore
      Id(idle(interpreter))

    case LockExpiryCheck =>
      // Ignore
      Id(idle(interpreter))

    case ProcessPendingRequests =>
      // Ignore
      Id(idle(interpreter))

  }

  private def pendingLocked(interpreter: PendingLockedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLocked(next)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLocked(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLocked(next)
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest))) if interpreter.lockedRequest == runningRequest =>
      val (responses, next) = interpreter.markLocked()
      for {
        r <- responses
        s <- next.notifySubscribers()
      } yield {
        (r ++ s).foreach(send)
        locked(next)
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      Id(pendingLocked(interpreter))

    case _: LockReturnRequest =>
      // Ignore
      Id(pendingLocked(interpreter))

    case LockExpiryCheck =>
      // Ignore
      Id(pendingLocked(interpreter))

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLocked(next)
      }
  }

  private def locked(interpreter: LockedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        locked(next)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        locked(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        locked(next)
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      Id(locked(interpreter))

    case v: LockReturnRequest if v.lock == interpreter.lockedRequest.lock =>
      val (responses, next) = interpreter.lockReturn()
      for {
        r <- responses
        s <- next.notifySubscribers()
      } yield {
        (r ++ s).foreach(send)
        pendingLockReturned(next)
      }

    case v: LockReturnRequest =>
      log.warning(s"Received unexpected message [$v] from [${sender()}]")
      Id(locked(interpreter))

    case LockExpiryCheck =>
      val (responses, next) = interpreter.checkExpiry()
      for {
        r <- responses
        s <- next match {
          case Left(_)  => Id(Interpreters.EmptyResponse)
          case Right(v) => v.notifySubscribers()
        }
      } yield {
        (r ++ s).foreach(send)
        next match {
          case Left(unexpired) => locked(unexpired)
          case Right(expired)  => pendingLockExpired(expired)
        }
      }

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      for {
        r <- responses
      } yield {
        r.foreach(send)
        locked(next)
      }
  }

  private def pendingLockReturned(interpreter: PendingLockReturnedStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockReturned(next)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockReturned(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockReturned(next)
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest))) if interpreter.returnedRequest == runningRequest =>
      val (responses, next) = interpreter.lockReturnConfirmed()
      for {
        r <- responses
        s <- next.fold(_.notifySubscribers(), _.notifySubscribers())
      } yield {
        (r ++ s).foreach(send)
        next match {
          case Left(v)  => idle(v)
          case Right(v) => pendingLocked(v)
        }
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      Id(pendingLockReturned(interpreter))

    case _: LockReturnRequest =>
      // Ignore
      Id(pendingLockReturned(interpreter))

    case LockExpiryCheck =>
      // Ignore
      Id(pendingLockReturned(interpreter))

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockReturned(next)
      }
  }

  private def pendingLockExpired(interpreter: PendingLockExpiredStateAlgo[Id]): Receive = onRequest {
    case v: LockGetRequest =>
      val (responses, next) = interpreter.lockRequest(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockExpired(next)
      }

    case v: SubscribeRequest =>
      val (responses, next) = interpreter.subscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockExpired(next)
      }

    case v: UnsubscribeRequest =>
      val (responses, next) = interpreter.unsubscribe(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockExpired(next)
      }

    case RecordLocksStorageMessageWrapper(RecordLocksStorage.UpdateStateSuccess(PendingLockExpiredState, Some(runningRequest))) if interpreter.expiredRequest == runningRequest =>
      val (responses, next) = interpreter.lockExpiryConfirmed()
      for {
        r <- responses
        s <- next.fold(_.notifySubscribers(), _.notifySubscribers())
      } yield {
        (r ++ s).foreach(send)
        next match {
          case Left(v)  => idle(v)
          case Right(v) => pendingLocked(v)
        }
      }

    case v: RecordLocksStorageMessageWrapper =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      Id(pendingLockExpired(interpreter))

    case v: LockReturnRequest if interpreter.expiredRequest.lock == v.lock =>
      val (responses, next) = interpreter.lockReturnedLate(v, sender())
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockExpired(next)
      }

    case v: LockReturnRequest =>
      log.warning(s"Unexpected message [$v] from [${sender()}]")
      Id(pendingLockExpired(interpreter))

    case LockExpiryCheck =>
      // Ignore
      Id(pendingLockExpired(interpreter))

    case ProcessPendingRequests =>
      val (responses, next) = interpreter.processPendingRequests()
      for {
        r <- responses
      } yield {
        r.foreach(send)
        pendingLockExpired(next)
      }
  }

  private def send(input: (ActorRef, _)): Unit =
    input._1 ! input._2

  private def onRequest(handler: RequestMessage => Id[Receive]): Receive = {
    case v: RequestMessage =>
      handler(v).map(context.become)

    case v: RecordLocksStorage.Message =>
      val wrapper = RecordLocksStorageMessageWrapper(v)
      self ! wrapper

    case Terminated(`storage`) =>
      context.stop(self)
  }

  private def createStorage(): ActorRef = ???
}
