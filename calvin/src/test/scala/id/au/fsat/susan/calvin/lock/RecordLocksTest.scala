package id.au.fsat.susan.calvin.lock

import java.time.Instant
import java.util.UUID

import akka.actor.{ ActorRef, Props }
import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.{ RecordId, UnitTest }
import org.scalatest.{ FunSpec, Inside }

import scala.concurrent.duration._
import scala.collection.immutable.Seq

class RecordLocksTest extends FunSpec with UnitTest with Inside {
  import RecordLocks._

  val maxTimeoutObtain = 1000.millis
  val maxTimeoutReturn = 5000.millis
  val removeStaleLocksAfter = 1000.millis
  val checkInterval = 100.millis
  val maxPendingRequests = 10

  describe("starting up") {
    it("transitions to idle state") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(IdleState, None, Seq.empty))

      client.awaitAssert {
        client.send(transactionLock, GetState)
        client.expectMsg(GetStateSuccess(IdleState, None, Seq.empty))
      }
    }

    it("transitions to pending lock obtained state, and then giving the locks to the client") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(PendingLockObtainedState, Some(runningRequest), Seq.empty))

      client1.expectMsg(LockGetSuccess(lock))

      client.awaitAssert {
        client.send(transactionLock, GetState)
        client.expectMsg(GetStateSuccess(LockedState, Some(runningRequest), Seq.empty))
      }
    }

    it("transitions to the locked state") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(LockedState, Some(runningRequest), Seq.empty))

      client1.expectNoMessage(100.millis)

      client1.send(transactionLock, LockReturnRequest(lock))
      client1.expectMsg(LockReturnSuccess(lock))
    }

    it("transitions to the pending lock expired state, and then send the expired message to the caller") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(PendingLockExpiredState, Some(runningRequest), Seq.empty))

      client1.expectMsg(LockExpired(lock))

      client.awaitAssert {
        client.send(transactionLock, GetState)
        client.expectMsg(GetStateSuccess(IdleState, None, Seq.empty))
      }
    }

    it("transitions to the pending lock returned state, and then send the returned message to the caller") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(PendingLockReturnedState, Some(runningRequest), Seq.empty))

      client1.expectMsg(LockReturnSuccess(lock))

      client.awaitAssert {
        client.send(transactionLock, GetState)
        client.expectMsg(GetStateSuccess(IdleState, None, Seq.empty))
      }
    }

    it("transitions to the next pending request state") {
      val f = testFixture()
      import f._

      client.send(transactionLock, GetState)
      client.expectMsg(GetStateSuccess(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
      val pendingRequest = RecordLocks.PendingRequest(client1.ref, request, createdAt = Instant.now())
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(NextPendingRequestState, None, Seq(pendingRequest)))

      val lock = inside(client1.expectMsgType[LockGetSuccess]) {
        case LockGetSuccess(l @ Lock(`requestId`, `recordId`, _, created, timeout)) =>
          created.plusNanos(timeoutReturn.toNanos) shouldBe timeout
          l
      }

      client.awaitAssert {
        client.send(transactionLock, GetState)
        inside(client.expectMsgType[GetStateSuccess]) {
          case GetStateSuccess(LockedState, Some(runningRequest), Seq()) =>
            runningRequest.caller shouldBe client1.ref
            runningRequest.request shouldBe request
            runningRequest.lock shouldBe lock
        }
      }
    }

    it("queues the incoming request while loading takes place")(pending)
  }

  describe("running") {
    describe("successful scenario") {
      describe("no existing lock") {
        it("obtains the lock") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val recordId = RecordId(1)

          client1.send(transactionLock, LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn))
          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `recordId`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))
          client1.expectMsg(LockReturnSuccess(lock))

          client2.expectNoMessage(100.millis)
        }

        it("allows locking from a different record as long as previous lock has been returned") {
          val f = testFixtureWithIdleStateOnStartup(maxTimeoutObtain = maxTimeoutObtain, maxTimeoutReturn = maxTimeoutReturn)
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record1 = RecordId(1)

          client1.send(transactionLock, LockGetRequest(requestId1, record1, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record1`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val record2 = RecordId(2)

          client2.send(transactionLock, LockGetRequest(requestId2, record2, maxTimeoutObtain, timeoutReturn))

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record2`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))
          client2.expectMsg(LockReturnSuccess(lock2))
        }
      }

      describe("existing locks in place") {
        it("waits for lock to be available for a particular record") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record = RecordId(1)

          client1.send(transactionLock, LockGetRequest(requestId1, record, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())

          client2.send(transactionLock, LockGetRequest(requestId2, record, maxTimeoutObtain, timeoutReturn))
          client2.expectNoMessage(50.millis)

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))
          client2.expectMsg(LockReturnSuccess(lock2))
        }
      }
    }

    describe("failure scenario") {
      describe("obtaining") {
        it("errors if the lock can't be obtained within specified timeout for a single record") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record = RecordId(1)

          client1.send(transactionLock, LockGetRequest(requestId1, record, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, record, 10.millis, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectMsg(LockGetTimeout(request2))

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          client2.expectNoMessage(300.millis)
        }

        it("errors if the lock to one of the record can't be obtained within specified timeout") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record1 = RecordId("ant")

          client1.send(transactionLock, LockGetRequest(requestId1, record1, timeoutObtain, timeoutReturn))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record1`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val record2 = RecordId("ant")
          val request2 = LockGetRequest(requestId2, record2, timeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectMsg(LockGetTimeout(request2))

          client1.send(transactionLock, LockReturnRequest(lock1))
          client1.expectMsg(LockReturnSuccess(lock1))

          client2.expectNoMessage(300.millis)
        }

        it("errors if the lock obtain timeout exceeds allowable max") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val record = RecordId(1)
          val request = LockGetRequest(requestId, record, maxTimeoutObtain + 1.milli, timeoutReturn)

          client1.send(transactionLock, request)
          inside(client1.expectMsgType[LockGetFailure]) {
            case LockGetFailure(`request`, error) =>
              error shouldBe an[IllegalArgumentException]
          }

          client2.expectNoMessage(300.millis)
        }

        it("errors if max pending requests is exceeded") {
          val f = testFixtureWithIdleStateOnStartup(maxPendingRequests = 1)
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record = RecordId(1)
          val request1 = LockGetRequest(requestId1, record, timeoutObtain, timeoutReturn)

          client1.send(transactionLock, request1)
          inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          // This request will be pending
          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, record, timeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectNoMessage(100.millis)

          // This request will be dropped
          val client3 = TestProbe()
          val requestId3 = RequestId(UUID.randomUUID())
          val request3 = LockGetRequest(requestId3, record, timeoutObtain, timeoutReturn)

          client3.send(transactionLock, request3)
          client3.expectMsg(LockGetRequestDropped(request3))
        }
      }

      describe("returning") {
        it("errors if the lock is returned after it expires") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val record = RecordId(1)

          // set return timeout to 0, effectively causing all locks to expire immediately after it's created
          client1.send(transactionLock, LockGetRequest(requestId, record, timeoutObtain, 0.millis))
          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `record`, _, createdAt, returnDeadline)) =>
              createdAt shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))
          client1.expectMsg(LockReturnLate(lock))

          client2.expectNoMessage(100.millis)
        }

        it("errors if the lock return timeout exceeds allowable max") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val record = RecordId(1)
          val request = LockGetRequest(requestId, record, timeoutObtain, maxTimeoutReturn + 1.millis)

          client1.send(transactionLock, request)
          inside(client1.expectMsgType[LockGetFailure]) {
            case LockGetFailure(`request`, error) =>
              error shouldBe an[IllegalArgumentException]
          }

          client2.expectNoMessage(300.millis)
        }

        it("errors when an unknown lock is returned") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val record = RecordId(1)
          val lock = Lock(requestId, record, UUID.randomUUID(), Instant.now(), Instant.now().plusSeconds(10))

          client1.send(transactionLock, LockReturnRequest(lock))
          inside(client1.expectMsgType[LockReturnFailure]) {
            case LockReturnFailure(`lock`, error) =>
              error shouldBe an[IllegalArgumentException]
          }

          client2.expectNoMessage(300.millis)
        }
      }

      describe("lock expiry") {
        it("allows obtaining new lock to the same record held by the expired old lock") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record = RecordId(1)

          client1.send(transactionLock, LockGetRequest(requestId1, record, timeoutObtain, 100.millis))
          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(100.millis.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, record, 10.millis, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectMsg(LockGetTimeout(request2))

          client1.expectMsg(LockExpired(lock1))

          client2.send(transactionLock, request2)
          inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }
        }
      }
    }
  }

  describe("shutting down") {
    it("terminates if the storage terminates") {
      val f = testFixture()
      import f._

      val monitorTerminates = TestProbe()
      monitorTerminates.watch(transactionLock)

      actorSystem.stop(mockStorage.ref)

      monitorTerminates.expectTerminated(transactionLock, 500.millis)
    }
  }

  private def testFixtureWithIdleStateOnStartup(maxTimeoutObtain: FiniteDuration = maxTimeoutObtain, maxTimeoutReturn: FiniteDuration = maxTimeoutReturn,
    removeStaleLocksAfter: FiniteDuration = removeStaleLocksAfter, checkInterval: FiniteDuration = checkInterval, maxPendingRequests: Int = maxPendingRequests) = {
    val fixture = testFixture(maxTimeoutObtain, maxTimeoutReturn, removeStaleLocksAfter, checkInterval, maxPendingRequests)

    import fixture._

    mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
    mockStorage.reply(RecordLocksStorage.GetStateSuccess(IdleState, None, Seq.empty))

    fixture
  }

  private def testFixture(maxTimeoutObtain: FiniteDuration = maxTimeoutObtain, maxTimeoutReturn: FiniteDuration = maxTimeoutReturn,
    removeStaleLocksAfter: FiniteDuration = removeStaleLocksAfter, checkInterval: FiniteDuration = checkInterval, maxPendingRequests: Int = maxPendingRequests) = new {
    val timeoutObtain = 300.millis
    val timeoutReturn = 2000.millis

    implicit val transactionLockSettings = RecordLockSettings(maxTimeoutObtain, maxTimeoutReturn, removeStaleLocksAfter, checkInterval, maxPendingRequests)

    val mockStorage = TestProbe()
    val transactionLock = actorSystem.actorOf(Props(new RecordLocks() {
      override protected def createRecordLocksStorage(): ActorRef = mockStorage.ref
    }))

    val client = TestProbe()
    val client1 = TestProbe()
    val client2 = TestProbe()
  }
}
