package id.au.fsat.susan.calvin.lock.interpreters

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.RecordLocks
import id.au.fsat.susan.calvin.lock.RecordLocks.RecordLocksState

object RecordLocksAlgo {
  import RecordLocks._

  type Responses = Seq[(ActorRef, ResponseMessage)]

  trait RecordLocksAlgoWithPendingRequests[F[_]] extends RecordLocksAlgo[F] {
    def lockRequest(req: LockGetRequest, sender: ActorRef): (Responses, Interpreter)
    def processPendingRequests(): (Responses, Interpreter)
  }

  trait LoadingStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: LoadingStateAlgo[F]
    override type State = LoadingState.type
    override val state = LoadingState

    def load(): (Responses, LoadingStateAlgo[F])
    def loaded(state: RecordLocksState, request: Option[RunningRequest]): (Responses, RecordLocksAlgo[F])
    def loadFailure(message: String, error: Option[Throwable]): (Responses, LoadingStateAlgo[F])
  }

  trait IdleStateAlgo[F[_]] extends RecordLocksAlgo[F] {
    override type Interpreter <: IdleStateAlgo[F]
    override type State = IdleState.type
    override val state = IdleState

    def lockRequest(req: LockGetRequest, sender: ActorRef): (Responses, Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
  }

  trait PendingLockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockedStateAlgo[F]
    override type State = PendingLockObtainedState.type
    override val state = PendingLockObtainedState

    import RecordLocks._

    def lockedRequest: RunningRequest
    def isWaitingForAck(request: RunningRequest): Boolean
    def markLocked(): (Responses, LockedStateAlgo[F])
  }

  trait LockedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: LockedStateAlgo[F]
    override type State = LockedState.type
    override val state = LockedState

    import RecordLocks._

    def lockedRequest: RunningRequest
    def isLockedRequestExpired: Boolean

    def checkExpiry(): (Responses, Either[LockedStateAlgo[F], PendingLockExpiredStateAlgo[F]])
    def lockReturnRequest(req: LockReturnRequest, sender: ActorRef): (Responses, PendingLockReturnedStateAlgo[F])
  }

  trait PendingLockReturnedStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockReturnedStateAlgo[F]
    override type State = PendingLockReturnedState.type
    override val state = PendingLockReturnedState

    def returnedRequest: RunningRequest
    def isWaitingForAck(request: RunningRequest): Boolean
    def lockReturnConfirmed(): (Responses, Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
  }

  trait PendingLockExpiredStateAlgo[F[_]] extends RecordLocksAlgoWithPendingRequests[F] {
    override type Interpreter <: PendingLockExpiredStateAlgo[F]
    override type State = PendingLockExpiredState.type
    override val state = PendingLockExpiredState

    def expiredRequest: RunningRequest
    def isWaitingForAck(request: RunningRequest): Boolean
    def lockExpiryConfirmed(): (Responses, Either[IdleStateAlgo[F], PendingLockedStateAlgo[F]])
    def lockReturnedLate(request: LockReturnRequest, sender: ActorRef): (Responses, PendingLockExpiredStateAlgo[F])
  }

}

trait RecordLocksAlgo[F[_]] {
  type Interpreter <: RecordLocksAlgo[F]
  type State <: RecordLocksState

  import RecordLocks._
  import RecordLocksAlgo.Responses

  def state: State

  def subscribe(req: SubscribeRequest, sender: ActorRef): (Responses, Interpreter)
  def unsubscribe(req: UnsubscribeRequest, sender: ActorRef): (Responses, Interpreter)
  def notifySubscribers(): Responses
}

