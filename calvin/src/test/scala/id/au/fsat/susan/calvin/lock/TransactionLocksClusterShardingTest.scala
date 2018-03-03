package id.au.fsat.susan.calvin.lock

import id.au.fsat.susan.calvin.ClusteredTest
import org.scalatest.FunSpec

class TransactionLocksClusterShardingTest extends FunSpec with ClusteredTest {
  describe("obtaining transaction lock") {
    it("waits for lock to be available for a particular record, while allowing locks to be obtained for other record")(pending)
  }

  describe("restarts") {
    it("replays transaction lock operation")(pending)
  }

  describe("transaction lock terminated") {
    it("terminates the transaction lock cluster sharding as well")(pending)
  }
}
