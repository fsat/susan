package id.au.fsat.susan.calvin.lock.programs

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.locks.{ LoadingStateAlgebra, LockStateAlgebra }
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra
import id.au.fsat.susan.calvin.lock.interpreters.storage.LockStorageAlgebra
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra
import id.au.fsat.susan.calvin.lock.messages.RequestMessage
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses

object Programs {

  case class Program(pf: PartialFunction[RequestMessage, (Id[Responses[_]], Program)])

  case class LoadingProgramState(
    loadingStateAlgebra: LoadingStateAlgebra[Id],
    lockStorageAlgebra: LockStorageAlgebra[Id],
    pendingRequestAlgebra: PendingRequestAlgebra[Id],
    subscriberAlgebra: SubscriberAlgebra[Id])

  def loading()(implicit state: LoadingProgramState): Program = Program {
    case v: SubscriberAlgebra.Messages.UnsubscribeRequest =>
      val (responses, next) = state.subscriberAlgebra.unsubscribe(v)
      responses.map(Seq(_)) -> loading()(state.copy(subscriberAlgebra = next))
  }

}
