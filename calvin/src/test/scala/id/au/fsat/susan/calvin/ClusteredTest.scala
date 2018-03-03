package id.au.fsat.susan.calvin

import akka.actor.{ ActorSystem, Address }
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{ Cluster, ClusterEvent, MemberStatus }
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import id.au.fsat.susan.calvin.ClusteredTest.ClusteredSetup
import org.scalatest.{ Matchers, Suite }

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

object ClusteredTest {
  case class ClusteredSetup(nodes: Seq[(ActorSystem, Cluster)])
}

trait ClusteredTest extends Matchers {
  this: Suite =>

  def withCluster[T](numberOfNodes: Int = 3, config: Config = createConfig())(callback: ClusteredSetup => T): T = {
    val nodes = (0 until numberOfNodes).map { _ =>
      val actorSystem = ActorSystem(this.getClass.getSimpleName, createConfig())
      actorSystem -> Cluster(actorSystem)
    }

    val (seedNode, seedNodeCluster) = nodes.head

    def nodeAddress(input: Cluster): Address = input.selfUniqueAddress.address

    val clusterListener = TestProbe()(seedNode)
    seedNodeCluster.subscribe(clusterListener.ref, ClusterEvent.InitialStateAsEvents, classOf[MemberUp])

    seedNodeCluster.joinSeedNodes(Seq(nodeAddress(seedNodeCluster)))

    val clusters = nodes.map(_._2)
    val (joinInfo, _) = clusters.tail.foldLeft((Seq.empty[(Cluster, Seq[Cluster])], Seq(seedNodeCluster))) { (result, entry) =>
      val (joinInfo, previousNodes) = result
      (joinInfo :+ (entry -> previousNodes), previousNodes :+ entry)
    }

    joinInfo.foreach { v =>
      val (cluster, otherMembers) = v
      cluster.joinSeedNodes(otherMembers.map(nodeAddress))
    }

    clusters.foreach { c =>
      TestProbe()(c.system).awaitAssert {
        c.state.members.count(_.status == MemberStatus.Up) shouldBe numberOfNodes
      }
    }

    val result = Try(callback(ClusteredSetup(nodes)))

    nodes.foreach { v =>
      val (actorSystem, _) = v
      Await.result(actorSystem.terminate(), Duration.Inf)
    }

    result.get
  }

  protected def createConfig(): Config = createRemotingConfig()

  protected def createRemotingConfig(): Config = {
    ConfigFactory.parseString(
      s"""
         |akka {
         |  actor {
         |    provider = "akka.cluster.ClusterActorRefProvider"
         |  }
         |  cluster {
         |      sharding.state-store-mode = ddata
         |  }
         |  remote {
         |    netty.tcp {
         |      hostname = "127.0.0.1"
         |      port = 0
         |    }
         |  }
         |}
         |
         |akka.cluster.jmx.multi-mbeans-in-same-jvm = on
       """.stripMargin)
  }
}
