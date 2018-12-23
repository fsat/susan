package id.au.fsat.susan.calvin.lock.interpreters.requests

import java.time.Instant

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.interpreters.locks.LockStateAlgebra.Messages._
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra.PendingRequest
import id.au.fsat.susan.calvin.lock.messages.RequestMessage.Request
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.{ResponseEither, Responses}

import scala.language.higherKinds

object PendingRequestAlgebra {
  final case class PendingRequest(caller: ActorRef, request: LockGetRequest, createdAt: Instant)
}

trait PendingRequestAlgebra[F[_]] {
  def pendingRequest: Seq[PendingRequest]
  def handleRequest(request: Request[LockGetRequest]): (F[ResponseEither[LockGetRequestInvalid, LockGetRequestEnqueued]], PendingRequestAlgebra[F])
  def expireStaleRequests(): (F[Responses[LockGetRequestDropped]], PendingRequestAlgebra[F])
}
