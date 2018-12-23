package id.au.fsat.susan.calvin.lock.messages

import akka.actor.ActorRef

object ResponseMessage {
  type Response[T <: ResponseMessage] = (ActorRef, T)
  type Responses[T <: ResponseMessage] = Seq[Response[T]]
}

trait ResponseMessage extends Message
