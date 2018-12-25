package id.au.fsat.susan.calvin.lock.messages

import akka.actor.ActorRef

object RequestMessage {
  type Request[T <: RequestMessage] = (ActorRef, T)
}

trait RequestMessage extends Message
