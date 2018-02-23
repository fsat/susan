package id.au.fsat.susan.calvin

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer }
import org.scalatest.{ BeforeAndAfterAll, Matchers, Suite }

import scala.concurrent.Await
import scala.concurrent.duration._

trait UnitTest extends BeforeAndAfterAll with Matchers {
  this: Suite =>

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    Await.ready(actorSystem.terminate(), Duration.Inf)
  }
}
