package id.au.fsat.susan.calvin

import akka.actor.{ ActorSystem, Address }
import akka.cluster.{ Cluster, ClusterEvent }
import akka.cluster.ClusterEvent.MemberUp
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ BeforeAndAfterAll, Matchers, Suite }

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.immutable.Seq

trait ClusteredTest extends BeforeAndAfterAll with Matchers {
  this: Suite =>

  def numberOfNodes: Int = 3

  val nodes = (0 to numberOfNodes).map(_ => ActorSystem(this.getClass.getSimpleName, createConfig()))
  val clusters = nodes.map(Cluster(_))

  val seedNode = nodes.head
  val seedNodeCluster = clusters.head

  override protected def beforeAll(): Unit = {
    def nodeAddress(input: Cluster): Address = input.selfUniqueAddress.address

    val clusterListener = TestProbe()(seedNode)
    seedNodeCluster.subscribe(clusterListener.ref, ClusterEvent.InitialStateAsEvents, classOf[MemberUp])

    seedNodeCluster.joinSeedNodes(Seq(nodeAddress(seedNodeCluster)))

    val (joinInfo, _) = clusters.tail.foldLeft((Seq.empty[(Cluster, Seq[Cluster])], Seq(seedNodeCluster))) { (result, entry) =>
      val (joinInfo, previousNodes) = result
      (joinInfo :+ (entry -> previousNodes), previousNodes :+ entry)
    }

    joinInfo.foreach { v =>
      val (cluster, otherMembers) = v
      cluster.joinSeedNodes(otherMembers.map(nodeAddress))
    }

    val clusterAddresses = clusters.map(nodeAddress)
    def isPartOfCluster(memberUp: MemberUp): Boolean =
      clusterAddresses.contains(memberUp.member.uniqueAddress.address)

    (0 to numberOfNodes).foreach { _ =>
      isPartOfCluster(clusterListener.expectMsgType[MemberUp]) shouldBe true
    }
  }

  override def afterAll(): Unit = {
    nodes.foreach { node =>
      Await.ready(node.terminate(), Duration.Inf)
    }
  }

  protected def createConfig(): Config = createRemotingConfig()

  protected def createRemotingConfig(): Config =
    ConfigFactory.parseString(
      s"""
         |akka {
         |  actor {
         |    provider = "cluster"
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
