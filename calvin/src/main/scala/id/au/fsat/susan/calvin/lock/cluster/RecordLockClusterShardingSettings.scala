package id.au.fsat.susan.calvin.lock.cluster

import akka.cluster.sharding.ShardRegion
import id.au.fsat.susan.calvin.RecordId
import id.au.fsat.susan.calvin.lock.cluster.RecordLockClusterShardingSettings._

object RecordLockClusterShardingSettings {
  type NumberOfShards = Int
  type RecordIdToEntityId = RecordId => String
  type RecordIdToShardId = NumberOfShards => RecordId => String
  type ShardEntityIdToShardId = NumberOfShards => ShardRegion.EntityId => String
}

case class RecordLockClusterShardingSettings(
  numberOfShards: Int,
  recordIdToEntityId: RecordIdToEntityId,
  recordIdToShardId: RecordIdToShardId,
  entityIdToShardId: ShardEntityIdToShardId) {
  val extractShardIdFromRecord: RecordId => String = recordIdToShardId(numberOfShards)
  val extractShardIdFromEntityId: ShardRegion.EntityId => String = entityIdToShardId(numberOfShards)
}
