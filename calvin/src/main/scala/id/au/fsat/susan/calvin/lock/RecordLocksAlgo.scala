package id.au.fsat.susan.calvin.lock

import akka.actor.ActorRef

object RecordLocksAlgo {
  import RecordLocks._

  type Responses = Seq[(ActorRef, ResponseMessage)]

  trait RecordLocksAlgoWithPendingRequests[F[_]] extends RecordLocksAlgo[F] {
    def lockRequest(req: LockGetRequest, sender: ActorRef): (Responses, Interpreter)
    def processPendingRequests(): (Responses, Interpreter)
  }

  trait LoadingStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    def loading(): Interpreter
    def loaded(): RecordLocksAlgo[F]
  }

  trait IdleStateAlgo[F[_]] extends RecordLocksAlgo[F] {
    def lockRequest(req: LockGetRequest, sender: ActorRef): (Responses, PendingLockedStateAlgo[F])
  }

  trait PendingLockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    import RecordLocks._

    def locked[Next <: LockedStateAlgo[F]](req: LockGetRequest, sender: ActorRef): (Responses, Next)
  }

  trait LockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    import RecordLocks._

    def lockedRequest: LockGetRequest
    def isLockedRequestExpired: Boolean

    def markExpired[Next <: PendingLockExpiredStateAlgo[F]](): (Responses, Next)
    def lockReturnRequest[Next <: PendingLockReturnedStateAlgo[F]](req: LockReturnRequest, sender: ActorRef): (Responses, Interpreter)
  }

  trait PendingLockReturnedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    def returnedRequest: LockGetRequest
    def lockReturnConfirmed[Next <: LockReturnedStateAlgo[F]](): Next
  }

  trait LockReturnedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    def returnedRequest: LockGetRequest
    def nextState[Idle <: IdleStateAlgo[F], PendingLocked <: PendingLockedStateAlgo[F]]: Either[Idle, PendingLocked]
  }

  trait PendingLockExpiredStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    def expiredRequest: LockGetRequest
    def lockExpiryConfirmed[Next <: LockReturnedStateAlgo[F]](): Next
  }

  trait LockExpiredStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    def expiredRequest: LockGetRequest
    def nextState[Idle <: IdleStateAlgo[F], PendingLocked <: PendingLockedStateAlgo[F]]: Either[Idle, PendingLocked]
  }

}

trait RecordLocksAlgo[F[_]] {
  type Interpreter <: RecordLocksAlgo[F]

  import RecordLocks._

  def value: F[_]

  def subscribe(req: SubscribeRequest, sender: ActorRef): (Option[ResponseMessage], Interpreter)
  def unsubscribe(req: UnsubscribeRequest, sender: ActorRef): (Option[ResponseMessage], Interpreter)
}

