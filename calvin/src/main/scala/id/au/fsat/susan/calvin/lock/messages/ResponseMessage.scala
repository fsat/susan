package id.au.fsat.susan.calvin.lock.messages

import akka.actor.ActorRef

object ResponseMessage {
  type Response[T <: ResponseMessage] = (ActorRef, T)
  type Responses[T <: ResponseMessage] = Seq[Response[T]]
  type ResponseEither[A <: ResponseMessage, B <: ResponseMessage] = Either[Response[A], Response[B]]
}

trait ResponseMessage extends Message
