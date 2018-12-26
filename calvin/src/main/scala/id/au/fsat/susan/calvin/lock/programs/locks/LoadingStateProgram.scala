package id.au.fsat.susan.calvin.lock.programs.locks

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.LoadingStateAlgebra
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.LockGetRequest
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages._
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
    case (_, _: GetStateSuccess) =>
      ???

    case (_, _: GetStateNotFound) =>
      val next = alg.loadedNoPriorState(s.pendingRequestAlgebra.pendingRequests)
        .left.map(IdleStateProgram.idle)
        .right.map(PendingLockedStateProgram.pendingLocked)
        .merge

      Id(Seq.empty) -> next

    case (_, v: GetStateFailure) =>
      val nextAttempt = attempt + 1
      val (response, next) = alg.retry(delay, nextAttempt)
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
