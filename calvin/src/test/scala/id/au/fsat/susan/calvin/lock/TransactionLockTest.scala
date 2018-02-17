package id.au.fsat.susan.calvin.lock

import org.scalatest.FunSpec

class TransactionLockTest extends FunSpec {
  describe("obtaining transaction lock") {
    describe("successful scenario") {
      it("obtains the lock")(pending)
      it("allows obtaining locks from multiple records concurrently")(pending)
    }

    describe("failure scenario") {
      describe("obtaining") {
        it("errors if the lock can't be obtained within specified timeout")
        it("errors if the lock obtain timeout exceeds allowable max")
      }

      describe("returning") {
        it("errors if the lock is returned after it expires")
        it("errors if the lock return timeout exceeds allowable max")
      }
    }
  }
}
