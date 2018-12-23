package id.au.fsat.susan.calvin.lockdeprecate

import java.time.Instant
import java.util.UUID

import akka.actor.{ ActorRef, Props }
import akka.testkit.TestProbe
import id.au.fsat.susan.calvin.lockdeprecate.storage.RecordLocksStorage
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
    describe("with existing state") {
      it("transitions to idle state") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
        mockStorage.reply(RecordLocksStorage.GetStateSuccess(IdleState, None))

        transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))
      }

      it("transitions to the locked state") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

        val requestId = RequestId(UUID.randomUUID())
        val recordId = RecordId(1)
        val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
        val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
        val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
        mockStorage.reply(RecordLocksStorage.GetStateSuccess(LockedState, Some(runningRequest)))

        transactionLockListener.expectMsg(StateChanged(LockedState, Some(runningRequest), Seq.empty))

        client1.expectNoMessage(100.millis)

        client1.send(transactionLock, LockReturnRequest(lock))

        mockStorage.expectMsgPF() {
          case RecordLocksStorage.UpdateStateRequest(`transactionLock`, PendingLockReturnedState, Some(r)) =>
            r shouldBe runningRequest
        }
        mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest)))

        client1.expectMsg(LockReturnSuccess(lock))
      }

      it("transitions to the pending lock expired state, and then send the expired message to the caller") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

        val requestId = RequestId(UUID.randomUUID())
        val recordId = RecordId(1)
        val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
        val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
        val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
        mockStorage.reply(RecordLocksStorage.GetStateSuccess(PendingLockExpiredState, Some(runningRequest)))

        transactionLockListener.expectMsg(StateChanged(PendingLockExpiredState, Some(runningRequest), Seq.empty))

        client1.expectMsg(LockExpired(lock))

        transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))
      }

      it("transitions to the pending lock returned state, and then send the returned message to the caller") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

        val requestId = RequestId(UUID.randomUUID())
        val recordId = RecordId(1)
        val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
        val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
        val runningRequest = RecordLocks.RunningRequest(client1.ref, request, createdAt = Instant.now().minusSeconds(1), lock)
        mockStorage.reply(RecordLocksStorage.GetStateSuccess(PendingLockReturnedState, Some(runningRequest)))

        transactionLockListener.expectMsg(StateChanged(PendingLockReturnedState, Some(runningRequest), Seq.empty))

        client1.expectMsg(LockReturnSuccess(lock))

        transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))
      }
    }

    describe("without existing state") {
      it("transitions to idle state") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
        mockStorage.reply(RecordLocksStorage.GetStateNotFound)

        transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))
      }

      it("transitions to next pending request") {
        val f = testFixture()
        import f._

        transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

        mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

        // send request

        val requestId = RequestId(UUID.randomUUID())
        val recordId = RecordId(1)
        val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)

        client1.send(transactionLock, request)

        transactionLockListener.expectMsgPF() {
          case StateChanged(LoadingState, None, Seq(pendingRequest)) =>
            pendingRequest.request shouldBe request
        }

        mockStorage.reply(RecordLocksStorage.GetStateNotFound)

        transactionLockListener.expectMsgPF() {
          case StateChanged(NextPendingRequestState, None, Seq(pendingRequest)) =>
            pendingRequest.request shouldBe request
        }
      }
    }
  }

  describe("running") {
    describe("successful scenario") {
      describe("no existing lock") {
        it("obtains the lock") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId = RequestId(UUID.randomUUID())
          val recordId = RecordId(1)
          val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)

          client1.send(transactionLock, request)

          val runningRequest = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request
              r
          }
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `recordId`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest)))

          client1.expectMsg(LockReturnSuccess(lock))

          client2.expectNoMessage(100.millis)

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, IdleState, None))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(IdleState, None))
        }

        it("allows locking from a different record as long as previous lock has been returned") {
          val f = testFixtureWithIdleStateOnStartup(maxTimeoutObtain = maxTimeoutObtain, maxTimeoutReturn = maxTimeoutReturn)
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record1 = RecordId(1)
          val request = LockGetRequest(requestId1, record1, timeoutObtain, timeoutReturn)

          client1.send(transactionLock, request)

          val runningRequest1 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request
              r

          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest1)))

          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record1`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val record2 = RecordId(2)
          val request2 = LockGetRequest(requestId2, record2, maxTimeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)

          client1.send(transactionLock, LockReturnRequest(lock1))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest1)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest1)))

          client1.expectMsg(LockReturnSuccess(lock1))

          val runningRequest2 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client2.ref
              r.request shouldBe request2
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest2)))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record2`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest2)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest2)))

          client2.expectMsg(LockReturnSuccess(lock2))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, IdleState, None))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(IdleState, None))
        }
      }

      describe("existing locks in place") {
        it("waits for lock to be available for a particular record") {
          val f = testFixtureWithIdleStateOnStartup()
          import f._

          val requestId1 = RequestId(UUID.randomUUID())
          val record = RecordId(1)
          val request1 = LockGetRequest(requestId1, record, timeoutObtain, timeoutReturn)

          client1.send(transactionLock, request1)

          val runningRequest1 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request1
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest1)))

          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, record, maxTimeoutObtain, timeoutReturn)

          client2.send(transactionLock, request2)
          client2.expectNoMessage(50.millis)

          client1.send(transactionLock, LockReturnRequest(lock1))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest1)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest1)))

          client1.expectMsg(LockReturnSuccess(lock1))

          val runningRequest2 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client2.ref
              r.request shouldBe request2
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest2)))

          val lock2 = inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
          }

          client2.send(transactionLock, LockReturnRequest(lock2))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest2)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest2)))

          client2.expectMsg(LockReturnSuccess(lock2))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, IdleState, None))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(IdleState, None))
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
          val request = LockGetRequest(requestId1, record, timeoutObtain, timeoutReturn)

          client1.send(transactionLock, request)

          val runningRequest = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

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

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest)))

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

          val runningRequest = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request1
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

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
          val request = LockGetRequest(requestId, record, timeoutObtain, 0.millis)

          // set return timeout to 0, effectively causing all locks to expire immediately after it's created
          client1.send(transactionLock, request)

          val runningRequest = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

          val lock = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId`, `record`, _, createdAt, returnDeadline)) =>
              createdAt shouldBe returnDeadline
              lock
          }

          client1.send(transactionLock, LockReturnRequest(lock))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockReturnedState, Some(runningRequest)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockReturnedState, Some(runningRequest)))

          client1.expectMsg(LockReturnLate(lock))

          client2.expectNoMessage(100.millis)

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, IdleState, None))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(IdleState, None))
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
          val request1 = LockGetRequest(requestId1, record, timeoutObtain, 100.millis)

          client1.send(transactionLock, request1)

          val runningRequest1 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client1.ref
              r.request shouldBe request1
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest1)))

          val lock1 = inside(client1.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId1`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(100.millis.toNanos) shouldBe returnDeadline
              lock
          }

          val requestId2 = RequestId(UUID.randomUUID())
          val request2 = LockGetRequest(requestId2, record, 10.millis, timeoutReturn)

          client2.send(transactionLock, request2)

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, PendingLockExpiredState, Some(runningRequest1)))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(PendingLockExpiredState, Some(runningRequest1)))

          client2.expectMsg(LockGetTimeout(request2))

          client1.expectMsg(LockExpired(lock1))

          mockStorage.expectMsg(RecordLocksStorage.UpdateStateRequest(transactionLock, IdleState, None))
          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(IdleState, None))

          client2.send(transactionLock, request2)

          val runningRequest2 = mockStorage.expectMsgPF() {
            case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
              r.caller shouldBe client2.ref
              r.request shouldBe request2
              r
          }

          mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest2)))

          inside(client2.expectMsgType[LockGetSuccess]) {
            case LockGetSuccess(lock @ Lock(`requestId2`, `record`, _, createdAt, returnDeadline)) =>
              createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
              lock
            case v =>
              println(v)
              fail()
          }
        }
      }
    }
  }

  describe("queueing incoming request") {
    it("works during loading state") {
      val f = testFixture()
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      // Send a request
      val requestIdPending = RequestId(UUID.randomUUID())
      val recordIdPending = RecordId(10)
      client1.send(transactionLock, LockGetRequest(requestIdPending, recordIdPending, timeoutObtain, timeoutReturn))

      transactionLockListener.expectMsgPF() {
        case StateChanged(LoadingState, None, Seq(pendingRequest)) =>
          pendingRequest.request.requestId shouldBe requestIdPending
          pendingRequest.request.recordId shouldBe recordIdPending
          pendingRequest.caller shouldBe client1.ref
      }
    }

    it(s"works during locked state") {
      val f = testFixture(maxPendingRequests = 1)
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1123)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, 3.minutes)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = Option(RecordLocks.RunningRequest(TestProbe().ref, request, createdAt = lock.createdAt, lock))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(LockedState, runningRequest))

      transactionLockListener.expectMsgPF() {
        case StateChanged(LockedState, `runningRequest`, Seq()) =>
      }

      val requestIdToEnqueue = RequestId(UUID.randomUUID())
      val recordIdToEnqueue = RecordId(12)
      // Force request to expire by negating the timeout to obtain
      val requestToEnqueue = LockGetRequest(requestIdToEnqueue, recordIdToEnqueue, maxTimeoutObtain, maxTimeoutReturn)

      client2.send(transactionLock, requestToEnqueue)

      transactionLockListener.expectMsgPF() {
        case StateChanged(LockedState, `runningRequest`, Seq(pendingRequest)) =>
          pendingRequest.request shouldBe requestToEnqueue
          pendingRequest.caller shouldBe client2.ref
      }
    }

    it(s"works during pending lock expired state") {
      val f = testFixtureWithIdleStateOnStartup(maxPendingRequests = 1)
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))

      val timeoutReturn = 200.millis

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1123)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)

      client1.send(transactionLock, request)

      val runningRequest = mockStorage.expectMsgPF() {
        case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
          r.caller shouldBe client1.ref
          r.request shouldBe request
          r
      }

      transactionLockListener.expectMsg(StateChanged(PendingLockObtainedState, Some(runningRequest), Seq.empty))

      mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

      transactionLockListener.expectMsg(StateChanged(LockedState, Some(runningRequest), Seq.empty))

      val lock = inside(client1.expectMsgType[LockGetSuccess]) {
        case LockGetSuccess(lock @ Lock(`requestId`, `recordId`, _, createdAt, returnDeadline)) =>
          createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
          lock
      }

      transactionLockListener.expectMsg(StateChanged(PendingLockExpiredState, Some(runningRequest), Seq.empty))

      mockStorage.expectMsgPF(timeoutReturn) {
        case RecordLocksStorage.UpdateStateRequest(`transactionLock`, PendingLockExpiredState, Some(`runningRequest`)) =>
      }

      val requestIdToEnqueue = RequestId(UUID.randomUUID())
      val recordIdToEnqueue = RecordId(12)
      // Force request to expire by negating the timeout to obtain
      val requestToEnqueue = LockGetRequest(requestIdToEnqueue, recordIdToEnqueue, maxTimeoutObtain, maxTimeoutReturn)

      client2.send(transactionLock, requestToEnqueue)

      transactionLockListener.expectMsgPF() {
        case StateChanged(PendingLockExpiredState, Some(`runningRequest`), Seq(pendingRequest)) =>
          pendingRequest.request shouldBe requestToEnqueue
          pendingRequest.caller shouldBe client2.ref
      }
    }

    it(s"works during pending lock returned state") {
      val f = testFixtureWithIdleStateOnStartup(maxPendingRequests = 1)
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      transactionLockListener.expectMsg(StateChanged(IdleState, None, Seq.empty))

      val timeoutReturn = 200.millis

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1123)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)

      client1.send(transactionLock, request)

      val runningRequest = mockStorage.expectMsgPF() {
        case RecordLocksStorage.UpdateStateRequest(`transactionLock`, LockedState, Some(r)) =>
          r.caller shouldBe client1.ref
          r.request shouldBe request
          r
      }

      transactionLockListener.expectMsg(StateChanged(PendingLockObtainedState, Some(runningRequest), Seq.empty))

      mockStorage.reply(RecordLocksStorage.UpdateStateSuccess(LockedState, Some(runningRequest)))

      transactionLockListener.expectMsg(StateChanged(LockedState, Some(runningRequest), Seq.empty))

      val lock = inside(client1.expectMsgType[LockGetSuccess]) {
        case LockGetSuccess(lock @ Lock(`requestId`, `recordId`, _, createdAt, returnDeadline)) =>
          createdAt.plusNanos(timeoutReturn.toNanos) shouldBe returnDeadline
          lock
      }

      client1.send(transactionLock, LockReturnRequest(lock))

      transactionLockListener.expectMsg(StateChanged(PendingLockReturnedState, Some(runningRequest), Seq.empty))

      mockStorage.expectMsgPF(timeoutReturn) {
        case RecordLocksStorage.UpdateStateRequest(`transactionLock`, PendingLockReturnedState, Some(`runningRequest`)) =>
      }

      val requestIdToEnqueue = RequestId(UUID.randomUUID())
      val recordIdToEnqueue = RecordId(12)
      // Force request to expire by negating the timeout to obtain
      val requestToEnqueue = LockGetRequest(requestIdToEnqueue, recordIdToEnqueue, maxTimeoutObtain, maxTimeoutReturn)

      client2.send(transactionLock, requestToEnqueue)

      transactionLockListener.expectMsgPF() {
        case StateChanged(PendingLockReturnedState, Some(`runningRequest`), Seq(pendingRequest)) =>
          pendingRequest.request shouldBe requestToEnqueue
          pendingRequest.caller shouldBe client2.ref
      }
    }
  }

  describe("stale pending request") {
    it("is timed out during loading state") {
      val f = testFixture()
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      // Send a request
      val requestIdPending = RequestId(UUID.randomUUID())
      val recordIdPending = RecordId(10)
      val request = LockGetRequest(requestIdPending, recordIdPending, maxTimeoutObtain, timeoutReturn)

      client1.send(transactionLock, request)
      client1.expectMsg(LockGetTimeout(request))
    }

    it(s"is timed out during locked state") {
      val f = testFixture(maxPendingRequests = 1)
      import f._

      transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

      val requestId = RequestId(UUID.randomUUID())
      val recordId = RecordId(1123)
      val request = LockGetRequest(requestId, recordId, timeoutObtain, 3.minutes)
      val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
      val runningRequest = RecordLocks.RunningRequest(TestProbe().ref, request, createdAt = lock.createdAt, lock)

      val requestIdPending = RequestId(UUID.randomUUID())
      val recordIdPending = RecordId(10)
      val requestPending = LockGetRequest(requestIdPending, recordIdPending, 100.millis, maxTimeoutReturn)

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
      mockStorage.reply(RecordLocksStorage.GetStateSuccess(LockedState, Some(runningRequest)))

      transactionLockListener.expectMsg(StateChanged(LockedState, Some(runningRequest), Seq.empty))

      // Hold the lock until all clients are dropped
      client1.send(transactionLock, requestPending)

      transactionLockListener.expectMsgPF() {
        case StateChanged(LockedState, Some(`runningRequest`), Seq(pendingRequest)) =>
          pendingRequest.request shouldBe requestPending
      }

      // This request will be rejected immediately
      val requestIdToReject = RequestId(UUID.randomUUID())
      val recordIdToReject = RecordId(12)
      // Force request to expire by negating the timeout to obtain
      val requestToReject = LockGetRequest(requestIdToReject, recordIdToReject, -1.second, maxTimeoutReturn)
      client2.send(transactionLock, requestToReject)
      client2.expectMsg(LockGetTimeout(requestToReject))

      client1.expectMsg(LockGetTimeout(requestPending))
    }

    Seq(
      "pending lock expired" -> PendingLockExpiredState,
      "pending lock returned" -> PendingLockReturnedState).foreach {
        case (scenario, state) =>
          it(s"is timed out during $scenario state") {
            val f = testFixture(maxPendingRequests = 1)
            import f._

            transactionLockListener.expectMsg(StateChanged(LoadingState, None, Seq.empty))

            val requestId = RequestId(UUID.randomUUID())
            val recordId = RecordId(1123)
            val request = LockGetRequest(requestId, recordId, timeoutObtain, 3.minutes)
            val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
            val runningRequest = RecordLocks.RunningRequest(TestProbe().ref, request, createdAt = lock.createdAt, lock)

            val requestIdPending = RequestId(UUID.randomUUID())
            val recordIdPending = RecordId(10)
            val requestPending = LockGetRequest(requestIdPending, recordIdPending, 10.millis, maxTimeoutReturn)

            mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

            // Hold the lock until all clients are dropped
            client1.send(transactionLock, requestPending)

            transactionLockListener.expectMsgPF() {
              case StateChanged(LoadingState, None, Seq(pendingRequest)) =>
                pendingRequest.request shouldBe requestPending
            }

            mockStorage.reply(RecordLocksStorage.GetStateSuccess(state, Some(runningRequest)))

            transactionLockListener.expectMsgPF() {
              case StateChanged(`state`, Some(`runningRequest`), Seq(pendingRequest)) =>
                pendingRequest.request shouldBe requestPending
            }

            client1.expectMsg(LockGetTimeout(requestPending))
          }
      }
  }

  describe("max pending request is reached") {
    it(s"drops request during loading state") {
      val f = testFixture(maxPendingRequests = 1)
      import f._

      mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))

      // Send a request
      val request1IdPending = RequestId(UUID.randomUUID())
      val record1IdPending = RecordId(10)
      val request1 = LockGetRequest(request1IdPending, record1IdPending, maxTimeoutObtain, timeoutReturn)

      client1.send(transactionLock, request1)

      val request2IdPending = RequestId(UUID.randomUUID())
      val record2IdPending = RecordId(11)
      val request2 = LockGetRequest(request2IdPending, record2IdPending, maxTimeoutObtain, timeoutReturn)

      client2.send(transactionLock, request2)
      client2.expectMsg(LockGetRequestDropped(request2))
    }

    Seq(
      "locked" -> LockedState,
      "pending lock expired" -> PendingLockExpiredState,
      "pending lock returned" -> PendingLockReturnedState).foreach {
        case (scenario, state) =>
          it(s"drops request during $scenario state") {
            val f = testFixture(maxPendingRequests = 1, maxTimeoutReturn = 3.minutes)
            import f._

            val requestId = RequestId(UUID.randomUUID())
            val recordId = RecordId(1123)
            val request = LockGetRequest(requestId, recordId, timeoutObtain, timeoutReturn)
            val lock = Lock(requestId, recordId, UUID.randomUUID(), Instant.now().minusSeconds(1), Instant.now().plusSeconds(5))
            val runningRequest = Option(RecordLocks.RunningRequest(TestProbe().ref, request, createdAt = lock.createdAt, lock))

            val requestIdPending = RequestId(UUID.randomUUID())
            val recordIdPending = RecordId(10)
            val requestPending = LockGetRequest(requestIdPending, recordIdPending, maxTimeoutObtain, maxTimeoutReturn)

            mockStorage.expectMsg(RecordLocksStorage.GetStateRequest(transactionLock))
            mockStorage.reply(RecordLocksStorage.GetStateSuccess(state, runningRequest))

            client1.send(transactionLock, requestPending)

            val requestIdToReject = RequestId(UUID.randomUUID())
            val recordIdToReject = RecordId(12)
            val requestToReject = LockGetRequest(requestIdToReject, recordIdToReject, maxTimeoutObtain, maxTimeoutReturn)

            client2.send(transactionLock, requestToReject)
            client2.expectMsg(LockGetRequestDropped(requestToReject))
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
    mockStorage.reply(RecordLocksStorage.GetStateSuccess(IdleState, None))

    fixture
  }

  private def testFixture(maxTimeoutObtain: FiniteDuration = maxTimeoutObtain, maxTimeoutReturn: FiniteDuration = maxTimeoutReturn,
    removeStaleLocksAfter: FiniteDuration = removeStaleLocksAfter, checkInterval: FiniteDuration = checkInterval, maxPendingRequests: Int = maxPendingRequests) = new {
    val timeoutObtain = 300.millis
    val timeoutReturn = 2.seconds

    val mockStorage = TestProbe()
    implicit val transactionLockSettings = RecordLockSettings(
      maxTimeoutObtain,
      maxTimeoutReturn,
      removeStaleLocksAfter,
      checkInterval,
      maxPendingRequests,
      createRecordLocksStorage = _ => mockStorage.ref)

    val transactionLock = actorSystem.actorOf(Props(new RecordLocks()))

    val client = TestProbe()
    val client1 = TestProbe()
    val client2 = TestProbe()

    val transactionLockListener = TestProbe()
    transactionLockListener.send(transactionLock, SubscribeRequest(transactionLockListener.ref))
    transactionLockListener.expectMsg(SubscribeSuccess)
  }

}