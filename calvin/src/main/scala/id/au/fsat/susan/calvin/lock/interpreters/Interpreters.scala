package id.au.fsat.susan.calvin.lock.interpreters

import java.time.Instant

import id.au.fsat.susan.calvin.lock.RecordLocks.PendingRequest

import scala.collection.immutable.Seq

object Interpreters {

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
