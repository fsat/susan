package id.au.fsat.susan.calvin.lock

import java.util.UUID

import akka.cluster.MemberStatus
import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.{ ClusteredTest, RecordId }
import id.au.fsat.susan.calvin.ClusteredTest.ClusteredSetup
import id.au.fsat.susan.calvin.lock.RecordLockClusterShardingSettings.{ RecordIdToEntityId, RecordIdToShardId, ShardEntityIdToShardId }
import id.au.fsat.susan.calvin.lock.RecordLocks._
import org.scalatest.{ FunSpec, Inside }

import scala.concurrent.Await
import scala.concurrent.duration._

object RecordLocksClusterShardingTest {
  val recordIdValueToString: RecordIdToEntityId = _.value.toString
  val shardFromRecord: RecordIdToShardId = shardCount => y => (y.value.toString.toLong % shardCount).toString
  val shardFromEntityId: ShardEntityIdToShardId = shardCount => y => (y.toString.toLong % shardCount).toString
}

class RecordLocksClusterShardingTest extends FunSpec with ClusteredTest with Inside {
  import RecordLocksClusterShardingTest._

  describe("obtaining transaction lock") {
    it("obtains the lock for a particular record") {
      withCluster() { implicit clusters =>
        val f = testFixture()
        import f._

        val timeoutObtain = f.txLockSettings.maxTimeoutObtain
        val timeoutReturn = f.txLockSettings.maxTimeoutReturn

        val client = TestProbe()(clusters.nodes.head._1)

        val requestId = RequestId(UUID.randomUUID())
        val recordId = RecordId(1)

        client.send(txLock1, LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn))

        val lock = inside(client.expectMsgType[LockGetSuccess]) {
          case LockGetSuccess(lock @ Lock(`requestId`, `recordId`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        client.send(txLock1, LockReturnRequest(lock))
        client.expectMsg(LockReturnSuccess(lock))
      }
    }

    it("waits for lock to be available for a particular record, while allowing locks to be obtained for other record") {
      withCluster() { implicit clusters =>
        val f = testFixture()
        import f._

        val timeoutObtain = f.txLockSettings.maxTimeoutObtain
        val timeoutReturn = f.txLockSettings.maxTimeoutReturn

        val client1 = TestProbe()(clusters.nodes.head._1)

        val request1Id = RequestId(UUID.randomUUID())
        val record1Id = RecordId(1)

        client1.send(txLock1, LockGetRequest(request1Id, record1Id, timeoutObtain, timeoutReturn))

        val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
          case LockGetSuccess(lock @ Lock(`request1Id`, `record1Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        val client2 = TestProbe()(clusters.nodes(1)._1)

        val request2Id = RequestId(UUID.randomUUID())
        val record2Id = RecordId(2)

        client2.send(txLock2, LockGetRequest(request2Id, record2Id, timeoutObtain, timeoutReturn))

        val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
          case LockGetSuccess(lock @ Lock(`request2Id`, `record2Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        val client3 = TestProbe()(clusters.nodes.last._1)

        val request3Id = RequestId(UUID.randomUUID())

        client3.send(txLock3, LockGetRequest(request3Id, record1Id, timeoutObtain, timeoutReturn))
        client3.expectNoMessage(100.millis)

        client2.send(txLock2, LockReturnRequest(lock2))
        client2.expectMsg(LockReturnSuccess(lock2))

        client1.send(txLock1, LockReturnRequest(lock1))
        client1.expectMsg(LockReturnSuccess(lock1))

        val lock3 = inside(client3.expectMsgType[LockGetSuccess](10.seconds)) {
          case LockGetSuccess(lock @ Lock(`request3Id`, `record1Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        client3.send(txLock3, LockReturnRequest(lock3))
        client3.expectMsg(LockReturnSuccess(lock3))
      }

    }
  }

  describe("a crash occurred") {
    it("continues transaction lock operation") {
      // TODO: re-instate test once CRDT is in place
      pending
      /*
      withCluster() { implicit clusters =>
        val f = testFixture()
        import f._

        val timeoutObtain = f.txLockSettings.maxTimeoutObtain
        val timeoutReturn = f.txLockSettings.maxTimeoutReturn

        val client1 = TestProbe()(clusters.nodes.head._1)

        val request1Id = RequestId(UUID.randomUUID())
        val record1Id = RecordId(1)

        client1.send(txLock1, LockGetRequest(request1Id, record1Id, timeoutObtain, timeoutReturn))

        val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
          case LockGetSuccess(lock @ Lock(`request1Id`, `record1Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        val client2 = TestProbe()(clusters.nodes(1)._1)

        val request2Id = RequestId(UUID.randomUUID())
        val record2Id = RecordId(2)

        client2.send(txLock2, LockGetRequest(request2Id, record2Id, timeoutObtain, timeoutReturn))

        val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
          case LockGetSuccess(lock @ Lock(`request2Id`, `record2Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        val client3 = TestProbe()(clusters.nodes.last._1)

        val request3Id = RequestId(UUID.randomUUID())

        client3.send(txLock3, LockGetRequest(request3Id, record1Id, timeoutObtain, timeoutReturn))
        client3.expectNoMessage(100.millis)

        // Crash the first actor system, (hopefully) taking down the shard on the first node
        Await.result(clusters.nodes.head._1.terminate(), Duration.Inf)
        // Down the first actor from other 2 nodes
        clusters.nodes.tail.foreach(_._2.down(clusters.nodes.head._2.selfUniqueAddress.address))

        client2.send(txLock2, LockReturnRequest(lock2))
        client2.expectMsg(LockReturnSuccess(lock2))

        clusters.nodes.tail.foreach { v =>
          TestProbe()(v._1).awaitAssert({
            v._2.state.members.count(_.status == MemberStatus.Up) shouldBe 2
          }, 10.seconds, 500.millis)
        }

        client3.send(txLock3, LockReturnRequest(lock1))
        client3.expectMsg(LockReturnSuccess(lock1))

        val lock3 = inside(client3.expectMsgType[LockGetSuccess](10.seconds)) {
          case LockGetSuccess(lock @ Lock(`request3Id`, `record1Id`, _, createdAt, returnDeadline)) =>
            createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
            lock
        }

        client3.send(txLock3, LockReturnRequest(lock3))
        client3.expectMsg(LockReturnSuccess(lock3))
      }
      */
    }
  }

  describe("transaction lock terminated") {
    it("terminates the transaction lock cluster sharding as well")(pending)
  }

  def testFixture(
    numberOfShards: Int = 3,
    recordIdToEntityId: RecordIdToEntityId = recordIdValueToString,
    recordIdToShardId: RecordIdToShardId = shardFromRecord,
    entityIdToShardId: ShardEntityIdToShardId = shardFromEntityId)(implicit clusteredSetup: ClusteredSetup) = new {

    implicit val txLockSettings = RecordLockSettings(
      maxTimeoutObtain = 10000.millis,
      maxTimeoutReturn = 30000.millis,
      removeStaleLockAfter = 500.millis,
      checkInterval = 100.millis,
      maxPendingRequests = 3)

    implicit val txLockShardingSettings = RecordLockClusterShardingSettings(
      numberOfShards = numberOfShards,
      recordIdToEntityId = recordIdToEntityId,
      recordIdToShardId = recordIdToShardId,
      entityIdToShardId = entityIdToShardId)

    val txLock1 = RecordLocksClusterSharding.create(clusteredSetup.nodes.head._1)
    val txLock2 = RecordLocksClusterSharding.create(clusteredSetup.nodes(1)._1)
    val txLock3 = RecordLocksClusterSharding.create(clusteredSetup.nodes.last._1)
  }
}
