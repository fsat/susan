package id.au.fsat.susan.calvin.lock.storage

import akka.cluster.ddata.{ DistributedData, Replicator }
import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.ClusteredTest
import org.scalatest.FunSpec

class RecordLocksStorageTest extends FunSpec with ClusteredTest {
  describe("CRDT get") {
    describe("with existing CRDT") {
      it("returns state if found")(pending)
      it("returns not found")(pending)
    }

    describe("with CRDT not present") {
      it("returns not found") {
        withCluster() { implicit clusters =>
          val f = testFixture
          import f._

          node2.client.send(node2.storage, RecordLocksStorage.GetStateRequest(node2.client.ref))
          node2.client.expectMsg(RecordLocksStorage.GetStateNotFound)
        }
      }
    }

    it("enqueues multiple requests")(pending)
  }

  private def testFixture(implicit setup: ClusteredTest.ClusteredSetup) = new {
    val node1 = new {
      implicit val (actorSystem, cluster) = setup.nodes.head
    }

    val node2 = new {
      implicit val (actorSystem, cluster) = setup.nodes(1)
      val storage = actorSystem.actorOf(RecordLocksStorage.props)
      val client = TestProbe()
    }

    val node3 = new {
      implicit val (actorSystem, cluster) = setup.nodes.last
      val replicator = DistributedData(actorSystem).replicator
    }
  }
}
