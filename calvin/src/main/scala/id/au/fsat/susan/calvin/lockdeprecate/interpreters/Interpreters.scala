package id.au.fsat.susan.calvin.lockdeprecate.interpreters

import java.time.Instant

import id.au.fsat.susan.calvin.lockdeprecate.RecordLocks.PendingRequest
import id.au.fsat.susan.calvin.lockdeprecate.interpreters.RecordLocksAlgo.Responses

import scala.collection.immutable.Seq

object Interpreters {
  val EmptyResponse: Responses = Seq.empty

  def now(): Instant = Instant.now()

  def filterExpired(now: Instant, pendingRequests: Seq[PendingRequest], maxPendingRequests: Int): (Seq[PendingRequest], Seq[PendingRequest], Seq[PendingRequest]) = {
    def timedOut(input: PendingRequest): Boolean = {
      val deadline = input.createdAt.plusNanos(input.request.timeoutObtain.toNanos)
      now.isAfter(deadline)
    }

    val pendingTimedOut = pendingRequests.filter(timedOut)
    val pendingAlive = pendingRequests.filterNot(timedOut)

    val pendingAliveSorted = pendingAlive.sortBy(_.createdAt)
    val pendingAliveKept = pendingAliveSorted.take(maxPendingRequests)
    val pendingAliveDropped = pendingAliveSorted.takeRight(pendingAliveSorted.length - maxPendingRequests)

    (pendingTimedOut, pendingAliveKept, pendingAliveDropped)
  }

}