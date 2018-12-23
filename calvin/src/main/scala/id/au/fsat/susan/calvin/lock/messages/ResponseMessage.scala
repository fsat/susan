package id.au.fsat.susan.calvin.lock.messages

import akka.actor.ActorRef

object ResponseMessage {
  type Responses[T <: ResponseMessage] = Seq[(ActorRef, T)]
}

trait ResponseMessage extends Message
