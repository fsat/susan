package id.au.fsat.susan.calvin.lock

import scala.concurrent.duration.FiniteDuration

/**
 * Transaction lock related settings.
 *
 * @param maxTimeoutObtain     Maximum allowable time to wait for [[RecordLocks.Lock]] to be available.
 * @param maxTimeoutReturn     Maximum allowable time for [[RecordLocks.Lock]] the lock to be returned.
 *                             Beyond this timeout, the lock is considered stale
 * @param removeStaleLockAfter The amount of time to wait before removing the stale locks.
 *                             Once the stale locks is removed from the system it becomes available.
 * @param checkInterval        The internal polling period to for removal of stale locks and processing of pending requests
 * @param maxPendingRequests   Maximum number of pending transaction lock request for a given
 *                             [[id.au.fsat.susan.calvin.RecordId]].
 */
case class RecordLockSettings(
  maxTimeoutObtain: FiniteDuration,
  maxTimeoutReturn: FiniteDuration,
  removeStaleLockAfter: FiniteDuration,
  checkInterval: FiniteDuration,
  maxPendingRequests: Int)
