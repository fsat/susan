package id.au.fsat.susan.calvin.lock

import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.persistence.PersistentActor

object TransactionLocksClusterSharding {
  val name = "transaction-locks-cluster-sharding"

  def createExtractEntityId()(implicit s: TransactionLockClusterShardingSettings): ShardRegion.ExtractEntityId = {
    case v: TransactionLocks.LockGetRequest =>
      s.recordIdToEntityId(v.recordId) -> v
  }

  def createExtractShardId()(implicit s: TransactionLockClusterShardingSettings): ShardRegion.ExtractShardId = {
    case v: TransactionLocks.LockGetRequest => s.extractShardIdFromRecord(v.recordId)
    case v: ShardRegion.StartEntity         => s.extractShardIdFromEntityId(v.entityId)
  }

  def create(system: ActorSystem)(implicit shardingSettings: TransactionLockClusterShardingSettings, s: TransactionLockSettings): ActorRef =
    create(system, customTransactionLocks = None)

  protected[susan] def create(system: ActorSystem, customTransactionLocks: Option[ActorRef])(implicit shardingSettings: TransactionLockClusterShardingSettings, s: TransactionLockSettings): ActorRef =
    ClusterSharding(system)
      .start(
        typeName = "TransactionLocksClusterSharding",
        entityProps = props(customTransactionLocks),
        settings = ClusterShardingSettings(system),
        extractEntityId = createExtractEntityId(),
        extractShardId = createExtractShardId())

  private def props(customTransactionLocks: Option[ActorRef] = None)(implicit s: TransactionLockSettings): Props =
    Props(new TransactionLocksClusterSharding(customTransactionLocks))
}

class TransactionLocksClusterSharding(customTransactionLocks: Option[ActorRef])(implicit s: TransactionLockSettings) extends PersistentActor with ActorLogging {
  val transactionLock: ActorRef = context.watch(
    customTransactionLocks.getOrElse {
      context.actorOf(TransactionLocks.props(), TransactionLocks.name)
    })

  override def receiveRecover: Receive = ???
  override def receiveCommand: Receive = ???
  override def persistenceId: String = ???
}
