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

  val node1: ActorSystem = ActorSystem(this.getClass.getSimpleName, createConfig())
  val node2: ActorSystem = ActorSystem(this.getClass.getSimpleName, createConfig())
  val node3: ActorSystem = ActorSystem(this.getClass.getSimpleName, createConfig())

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)

  override protected def beforeAll(): Unit = {
    def nodeAddresses(input: Cluster*): Seq[Address] = input.toList.map(_.selfUniqueAddress.address)

    val clusterListener = TestProbe()(node1)
    node1Cluster.subscribe(clusterListener.ref, ClusterEvent.InitialStateAsEvents, classOf[MemberUp])

    node1Cluster.joinSeedNodes(nodeAddresses(node1Cluster))
    node2Cluster.joinSeedNodes(nodeAddresses(node1Cluster))
    node3Cluster.joinSeedNodes(nodeAddresses(node1Cluster, node2Cluster))

    val clusterAddresses = nodeAddresses(node1Cluster, node2Cluster, node3Cluster)
    def isPartOfCluster(memberUp: MemberUp): Boolean =
      clusterAddresses.contains(memberUp.member.uniqueAddress.address)

    isPartOfCluster(clusterListener.expectMsgType[MemberUp]) shouldBe true
    isPartOfCluster(clusterListener.expectMsgType[MemberUp]) shouldBe true
    isPartOfCluster(clusterListener.expectMsgType[MemberUp]) shouldBe true
  }

  override def afterAll(): Unit = {
    Await.ready(node1.terminate(), Duration.Inf)
    Await.ready(node2.terminate(), Duration.Inf)
    Await.ready(node3.terminate(), Duration.Inf)
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
