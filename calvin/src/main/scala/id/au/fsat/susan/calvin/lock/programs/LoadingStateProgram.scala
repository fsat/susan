package id.au.fsat.susan.calvin.lock.programs

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.{ IdleStateAlgebra, LoadingStateAlgebra }
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages.LockGetRequest
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses

object LoadingStateProgram {

  def start(alg: LoadingStateAlgebra[Id])(implicit s: CommonStates): (Id[Responses[_]], Program[Id]) = {
    val (response, next) = alg.load()
    response.map(Seq(_)) -> loading(next)
  }

  private def loading(alg: LoadingStateAlgebra[Id])(implicit s: CommonStates): Program[Id] = Program {
    case (_, v: GetStateNotFound) =>
      val next = alg.loadedNoPriorState(s.pendingRequestAlgebra.pendingRequests)

      val nextProgram = next match {
        case a: IdleStateAlgebra[Id] @unchecked =>
          IdleStateProgram.idle(a)

        // TODO: all other algebras
      }

      Id(Seq.empty) -> nextProgram
    case (_, v: GetStateSuccess) =>
      ???

    case (_, v: GetStateFailure) =>
      // TODO: logging + exponential backoff
      start(alg)

    case (sender, v: LockGetRequest) =>
      val (responses, next) = s.pendingRequestAlgebra.handleRequest(sender -> v)
      responses.map(v => Seq(v.merge)) -> loading(alg)(s.copy(pendingRequestAlgebra = next))

    case (_, v: SubscriberAlgebra.Messages.SubscribeRequest) =>
      val (response, next) = s.subscriberAlgebra.subscribe(
        request = v,
        runningRequest = None,
        pendingRequests = s.pendingRequestAlgebra.pendingRequests)
      response.map(Seq(_)) -> loading(alg)(s.copy(subscriberAlgebra = next))

    case (_, v: SubscriberAlgebra.Messages.UnsubscribeRequest) =>
      val (response, next) = s.subscriberAlgebra.unsubscribe(v)
      response.map(Seq(_)) -> loading(alg)(s.copy(subscriberAlgebra = next))
  }

}
