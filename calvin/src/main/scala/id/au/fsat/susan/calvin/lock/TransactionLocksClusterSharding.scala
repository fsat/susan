package id.au.fsat.susan.calvin.lock

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }

object TransactionLocksClusterSharding {
  val name = "transaction-locks-cluster-sharding"

  def createExtractEntityId()(implicit s: TransactionLockClusterShardingSettings): ShardRegion.ExtractEntityId = {
    case v: TransactionLocks.LockGetRequest    => s.recordIdToEntityId(v.recordId) -> v
    case v: TransactionLocks.LockReturnRequest => s.recordIdToEntityId(v.lock.recordId) -> v
  }

  def createExtractShardId()(implicit s: TransactionLockClusterShardingSettings): ShardRegion.ExtractShardId = {
    case v: TransactionLocks.LockGetRequest    => s.extractShardIdFromRecord(v.recordId)
    case v: TransactionLocks.LockReturnRequest => s.extractShardIdFromRecord(v.lock.recordId)
    case v: ShardRegion.StartEntity            => s.extractShardIdFromEntityId(v.entityId)
  }

  def create(system: ActorSystem)(implicit shardingSettings: TransactionLockClusterShardingSettings, s: TransactionLockSettings): ActorRef =
    ClusterSharding(system)
      .start(
        typeName = "TransactionLocks",
        entityProps = props(),
        settings = ClusterShardingSettings(system),
        extractEntityId = createExtractEntityId(),
        extractShardId = createExtractShardId())

  private def props()(implicit s: TransactionLockSettings): Props =
    Props(new TransactionLocksClusterSharding())
}

class TransactionLocksClusterSharding(implicit s: TransactionLockSettings) extends Actor with ActorLogging {
  val transactionLock: ActorRef = context.watch(context.actorOf(TransactionLocks.props(), TransactionLocks.name))

  override def receive: Receive = {
    case v: TransactionLocks.RequestMessage =>
      transactionLock.forward(v)
  }
}
