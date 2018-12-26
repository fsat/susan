package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks._
import id.au.fsat.susan.calvin.lock.interpreters.locks.LoadingStateAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.LockGetRequest
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses
import id.au.fsat.susan.calvin.lock.programs.Program

import scala.concurrent.duration._

object LoadingStateProgram {

  def start(alg: LoadingStateAlgebra[Id], delay: FiniteDuration, attempt: Int)(implicit s: CommonStates): (Id[Responses[_]], Program[Id]) = {
    val (response, next) = alg.load()
    response.map(Seq(_)) -> loading(next, delay, attempt)
  }

  private[programs] def loading(alg: LoadingStateAlgebra[Id], delay: FiniteDuration, attempt: Int)(implicit s: CommonStates): Program[Id] = Program[Id] {
    case (sender, v: LoadedState) =>
      val next = alg.loaded(sender -> v, s.pendingRequestAlgebra.pendingRequests)
      val (responses, nextSubscriber) =
        s.subscriberAlgebra.stateChange(next.currentState, runningRequest = None, s.pendingRequestAlgebra.pendingRequests)

      val commonStatesNext = s.copy(subscriberAlgebra = nextSubscriber)

      val nextProgram =
        next match {
          case a: IdleStateAlgebra[Id] @unchecked           => IdleStateProgram.idle(a)(commonStatesNext)
          case a: LockedStateAlgebra[Id] @unchecked         => LockedStateProgram.locked(a)(commonStatesNext)
          case a: PendingLockExpiredAlgebra[Id] @unchecked  => PendingLockExpiredStateProgram.pendingLockExpired(a)(commonStatesNext)
          case a: PendingLockReturnedAlgebra[Id] @unchecked => PendingLockReturnedStateProgram.pendingLockReturned(a)(commonStatesNext)
        }

      responses -> nextProgram

    case (_, v: LoadFailure) =>
      val nextAttempt = attempt + 1
      val (response, next) = alg.retry(v, delay, nextAttempt)
      response.map(Seq(_)) -> loading(next, delay, nextAttempt)

    case (sender, v: LockGetRequest) =>
      val (responses, next) = s.pendingRequestAlgebra.handleRequest(sender -> v)
      responses.map(v => Seq(v.merge)) -> loading(alg, delay, attempt)(s.copy(pendingRequestAlgebra = next))

    case (_, v: SubscriberAlgebra.Messages.SubscribeRequest) =>
      val (response, next) = s.subscriberAlgebra.subscribe(
        request = v,
        runningRequest = None,
        pendingRequests = s.pendingRequestAlgebra.pendingRequests)
      response.map(Seq(_)) -> loading(alg, delay, attempt)(s.copy(subscriberAlgebra = next))

    case (_, v: SubscriberAlgebra.Messages.UnsubscribeRequest) =>
      val (response, next) = s.subscriberAlgebra.unsubscribe(v)
      response.map(Seq(_)) -> loading(alg, delay, attempt)(s.copy(subscriberAlgebra = next))
  }

}
