package id.au.fsat.susan.calvin.lock

import java.util.UUID

import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.{ ClusteredTest, RecordId }
import id.au.fsat.susan.calvin.ClusteredTest.ClusteredSetup
import id.au.fsat.susan.calvin.lock.TransactionLockClusterShardingSettings.{ RecordIdToEntityId, RecordIdToShardId, ShardEntityIdToShardId }
import id.au.fsat.susan.calvin.lock.TransactionLocks._
import org.scalatest.{ FunSpec, Inside }

import scala.concurrent.duration._

object TransactionLocksClusterShardingTest {
  val recordIdValueToString: RecordIdToEntityId = _.value.toString
  val shardFromRecord: RecordIdToShardId = x => y => (y.value.hashCode() % x).toString
  val shardFromEntityId: ShardEntityIdToShardId = x => y => (y.hashCode() % x).toString
}

class TransactionLocksClusterShardingTest extends FunSpec with ClusteredTest with Inside {
  import TransactionLocksClusterShardingTest._

  describe("obtaining transaction lock") {
    it("obtains the lock for a particular record") {
      withCluster() { implicit cluster =>
        val f = testFixture()
        import f._

        val timeoutObtain = f.txLockSettings.maxTimeoutObtain
        val timeoutReturn = f.txLockSettings.maxTimeoutReturn

        val client = TestProbe()(cluster.nodes.last._1)

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

    it("waits for lock to be available for a particular record, while allowing locks to be obtained for other record")(pending)
  }

  describe("restarts") {
    it("replays transaction lock operation")(pending)
  }

  describe("transaction lock terminated") {
    it("terminates the transaction lock cluster sharding as well")(pending)
  }

  def testFixture(
    numberOfShards: Int = 3,
    recordIdToEntityId: RecordIdToEntityId = recordIdValueToString,
    recordIdToShardId: RecordIdToShardId = shardFromRecord,
    entityIdToShardId: ShardEntityIdToShardId = shardFromEntityId)(implicit clusteredSetup: ClusteredSetup) = new {

    implicit val txLockSettings = TransactionLockSettings(
      maxTimeoutObtain = 2000.millis,
      maxTimeoutReturn = 10000.millis,
      removeStaleLockAfter = 500.millis,
      checkInterval = 100.millis,
      maxPendingRequests = 3)

    implicit val txLockShardingSettings = TransactionLockClusterShardingSettings(
      numberOfShards = numberOfShards,
      recordIdToEntityId = recordIdToEntityId,
      recordIdToShardId = recordIdToShardId,
      entityIdToShardId = entityIdToShardId)

    val txLock1 = TransactionLocksClusterSharding.create(clusteredSetup.nodes.head._1)
    val txLock2 = TransactionLocksClusterSharding.create(clusteredSetup.nodes(1)._1)
    val txLock3 = TransactionLocksClusterSharding.create(clusteredSetup.nodes.last._1)
  }
}
