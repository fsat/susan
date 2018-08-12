package id.au.fsat.susan.calvin.lock.interpreters

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks.RecordLocksState

object RecordLocksAlgo {
  import RecordLocks._

  // TODO: do we need to tighten the `Any`?
  type Responses = Seq[(ActorRef, Any)]

  trait RecordLocksAlgoWithPendingRequests[F[_]] extends RecordLocksAlgo[F] {
    def lockRequest(req: LockGetRequest, sender: ActorRef): (F[Responses], Interpreter)
    def processPendingRequests(): (F[Responses], Interpreter)
  }

  trait LoadingStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: LoadingStateAlgo[F]
    override type State = LoadingState.type
    override val state = LoadingState

    def load(): (Responses, LoadingStateAlgo[F])
    def loaded(state: RecordLocksState, request: Option[RunningRequest]): (F[Responses], RecordLocksAlgo[F])
    def loadFailure(message: String, error: Option[Throwable]): (F[Responses], LoadingStateAlgo[F])
  }

  trait IdleStateAlgo[F[_]] extends RecordLocksAlgo[F] {
    override type Interpreter <: IdleStateAlgo[F]
    override type State = IdleState.type
    override val state = IdleState

    def lockRequest(req: LockGetRequest, sender: ActorRef): (F[Responses], Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
  }

  trait PendingLockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockedStateAlgo[F]
    override type State = PendingLockObtainedState.type
    override val state = PendingLockObtainedState

    import RecordLocks._

    def lockedRequest: RunningRequest
    def markLocked(): (F[Responses], LockedStateAlgo[F])
  }

  trait LockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: LockedStateAlgo[F]
    override type State = LockedState.type
    override val state = LockedState

    import RecordLocks._

    def lockedRequest: RunningRequest
    def checkExpiry(): (F[Responses], Either[LockedStateAlgo[F], PendingLockExpiredStateAlgo[F]])
    def lockReturn(): (F[Responses], PendingLockReturnedStateAlgo[F])
  }

  trait PendingLockReturnedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockReturnedStateAlgo[F]
    override type State = PendingLockReturnedState.type
    override val state = PendingLockReturnedState

    def returnedRequest: RunningRequest
    def lockReturnConfirmed(): (F[Responses], Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
  }

  trait PendingLockExpiredStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockExpiredStateAlgo[F]
    override type State = PendingLockExpiredState.type
    override val state = PendingLockExpiredState

    def expiredRequest: RunningRequest
    def lockExpiryConfirmed(): (F[Responses], Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
    def lockReturnedLate(request: LockReturnRequest, sender: ActorRef): (F[Responses], PendingLockExpiredStateAlgo[F])
  }

}

trait RecordLocksAlgo[F[_]] {
  type Interpreter <: RecordLocksAlgo[F]
  type State <: RecordLocksState

  import RecordLocks._
  import RecordLocksAlgo.Responses

  def state: F[State]

  def subscribe(req: SubscribeRequest, sender: ActorRef): (F[Responses], Interpreter)
  def unsubscribe(req: UnsubscribeRequest, sender: ActorRef): (F[Responses], Interpreter)
  def notifySubscribers(): F[Responses]
}

