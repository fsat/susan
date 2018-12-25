package id.au.fsat.susan.calvin.lock.programs

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.messages.RequestMessage
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses
import id.au.fsat.susan.calvin.lock.programs.Program.Sender

object Program {
  type Sender = ActorRef
}

case class Program(pf: PartialFunction[(Sender, RequestMessage), (Id[Responses[_]], Program)])