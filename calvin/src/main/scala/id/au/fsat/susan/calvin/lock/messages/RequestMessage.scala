package id.au.fsat.susan.calvin.lock.messages

import akka.actor.ActorRef

object RequestMessage {
  type Request[T <: RequestMessage] = (T, ActorRef)
}

trait RequestMessage extends Message
