package id.au.fsat.susan.calvin.lock

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }

object RecordLocksClusterSharding {
  val name = "transaction-locks-cluster-sharding"

  def createExtractEntityId()(implicit s: RecordLockClusterShardingSettings): ShardRegion.ExtractEntityId = {
    case v: RecordLocks.LockGetRequest    => s.recordIdToEntityId(v.recordId) -> v
    case v: RecordLocks.LockReturnRequest => s.recordIdToEntityId(v.lock.recordId) -> v
  }

  def createExtractShardId()(implicit s: RecordLockClusterShardingSettings): ShardRegion.ExtractShardId = {
    case v: RecordLocks.LockGetRequest    => s.extractShardIdFromRecord(v.recordId)
    case v: RecordLocks.LockReturnRequest => s.extractShardIdFromRecord(v.lock.recordId)
    case v: ShardRegion.StartEntity       => s.extractShardIdFromEntityId(v.entityId)
  }

  def create(system: ActorSystem)(implicit shardingSettings: RecordLockClusterShardingSettings, s: RecordLockSettings): ActorRef =
    ClusterSharding(system)
      .start(
        typeName = "TransactionLocks",
        entityProps = props(),
        settings = ClusterShardingSettings(system),
        extractEntityId = createExtractEntityId(),
        extractShardId = createExtractShardId())

  private def props()(implicit s: RecordLockSettings): Props =
    Props(new RecordLocks())

  case class ReceivedMessage(sender: ActorRef, request: RecordLocks.RequestMessage)
}
