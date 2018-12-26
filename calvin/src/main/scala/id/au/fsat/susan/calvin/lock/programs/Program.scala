package id.au.fsat.susan.calvin.lock.programs

import akka.actor.ActorRef
import id.au.fsat.susan.calvin.lock.messages.RequestMessage
import id.au.fsat.susan.calvin.lock.messages.ResponseMessage.Responses
import id.au.fsat.susan.calvin.lock.programs.Program.Sender

import scala.language.higherKinds

object Program {
  type Sender = ActorRef
}

case class Program[F[_]](pf: Function[(Sender, RequestMessage), (F[Responses[_]], Program[F])])