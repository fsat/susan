package id.au.fsat.susan.calvin.lock

import java.util.UUID

import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.{ RecordId, UnitTest }
import org.scalatest.{ FunSpec, Inside }

import scala.concurrent.duration._

class TransactionLockTest extends FunSpec with UnitTest with Inside {
  import TransactionLock._

  val maxTimeoutObtain: FiniteDuration = 500.millis
  val maxTimeoutReturn: FiniteDuration = 5000.millis

  describe("obtaining transaction lock") {
    describe("successful scenario") {
      describe("no existing lock") {
        it("obtains the lock") {
          val f = testFixture()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1))

          client1.send(transactionLock, LockGetRequest(requestId, records, timeoutObtain, timeoutReturn))
          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))
          client1.expectMsg(LockReturnSuccess(lock))

          client2.expectNoMessage(100.millis)
        }

        it("allows obtaining locks from multiple records concurrently") {
          val f = testFixture()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1), RecordId("joe"))

          client1.send(transactionLock, LockGetRequest(requestId, records, timeoutObtain, timeoutReturn))
          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))
          client1.expectMsg(LockReturnSuccess(lock))

          client2.expectNoMessage(100.millis)

        }
      }

      describe("existing lock in place") {
        it("waits for lock to be available for a particular record") {
          val f = testFixture()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1))

          client1.send(transactionLock, LockGetRequest(requestId1, records, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())

          client2.send(transactionLock, LockGetRequest(requestId2, records, timeoutObtain, timeoutReturn))
          client2.expectNoMessage(timeoutReturn / 2)

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))
          client2.expectMsg(LockReturnSuccess(lock2))
        }

        it("waits for lock to be available for multiple records") {
          val f = testFixture()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1), RecordId("ant"))

          client1.send(transactionLock, LockGetRequest(requestId1, records, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())

          client2.send(transactionLock, LockGetRequest(requestId2, records, timeoutObtain, timeoutReturn))
          client2.expectNoMessage(timeoutReturn / 2)

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))
          client2.expectMsg(LockReturnSuccess(lock2))
        }

        it("should not wait if transaction lock is requested for different records") {
          val f = testFixture()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val records1 = Vector(RecordId(1), RecordId("ant"))

          client1.send(transactionLock, LockGetRequest(requestId1, records1, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records1`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val records2 = Vector(RecordId(10), RecordId("jim"))

          client2.send(transactionLock, LockGetRequest(requestId2, records1, timeoutObtain, timeoutReturn))
          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `records2`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          client2.send(transactionLock, LockReturnRequest(lock2))
          client2.expectMsg(LockReturnSuccess(lock2))
        }
      }
    }

    describe("failure scenario") {
      describe("obtaining") {
        it("errors if the lock can't be obtained within specified timeout for a single record") {
          val f = testFixture()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1), RecordId("ant"))

          client1.send(transactionLock, LockGetRequest(requestId1, records, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, records, timeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectMsg(LockGetTimeout(request2))

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          client2.expectNoMessage(300.millis)
        }

        it("errors if the lock to one of the record can't be obtained within specified timeout") {
          val f = testFixture()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val records1 = Vector(RecordId(1), RecordId("ant"))

          client1.send(transactionLock, LockGetRequest(requestId1, records1, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `records1`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val records2 = Vector(RecordId(101), RecordId("ant"))
          val request2 = LockGetRequest(requestId2, records2, timeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectMsg(LockGetTimeout(request2))

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          client2.expectNoMessage(300.millis)
        }

        it("errors if the lock obtain timeout exceeds allowable max") {
          val f = testFixture()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1), RecordId("ant"))
          val request = LockGetRequest(requestId, records, maxTimeoutObtain + 1.milli, timeoutReturn)

          client1.send(transactionLock, request)
          inside(client1.expectMsgType[LockGetFailure]) {
            case LockGetFailure(`request`, error) =>
              error shouldBe an[IllegalArgumentException]
          }

          client2.expectNoMessage(300.millis)
        }
      }

      describe("returning") {
        it("errors if the lock is returned after it expires") {
          val f = testFixture()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1))

          // set return timeout to 0, effectively causing all locks to expire immediately after it's created
          client1.send(transactionLock, LockGetRequest(requestId, records, timeoutObtain, 0.millis))
          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `records`, _, createdAt, returnDeadline)) =>
              createdAt shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))
          client1.expectMsg(LockReturnLate(lock))

          client2.expectNoMessage(100.millis)
        }

        it("errors if the lock return timeout exceeds allowable max") {
          val f = testFixture()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val records = Vector(RecordId(1), RecordId("ant"))
          val request = LockGetRequest(requestId, records, timeoutObtain, maxTimeoutReturn + 1.millis)

          client1.send(transactionLock, request)
          inside(client1.expectMsgType[LockGetFailure]) {
            case LockGetFailure(`request`, error) =>
              error shouldBe an[IllegalArgumentException]
          }

          client2.expectNoMessage(300.millis)
        }

        it("errors when an unknown lock is returned")(pending)
      }

      describe("lock expiry") {
        it("allows obtaining new lock to the same record held by the expired old lock")(pending)
      }
    }
  }

  private def testFixture(maxTimeoutObtain: FiniteDuration = maxTimeoutObtain, maxTimeoutReturn: FiniteDuration = maxTimeoutReturn) = new {
    val timeoutObtain = 300.millis
    val timeoutReturn = 2000.millis

    val transactionLock = actorSystem.actorOf(TransactionLock.props(maxTimeoutObtain, maxTimeoutReturn))

    val client1 = TestProbe()
    val client2 = TestProbe()
  }
}
