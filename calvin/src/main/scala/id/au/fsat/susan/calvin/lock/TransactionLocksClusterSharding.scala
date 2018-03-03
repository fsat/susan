package id.au.fsat.susan.calvin.lock

import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.Cluster
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.persistence.{ PersistentActor, SnapshotOffer }
import id.au.fsat.susan.calvin.lock.TransactionLocksClusterSharding.ReceivedMessage

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

  case class ReceivedMessage(sender: ActorRef, request: TransactionLocks.RequestMessage)
}

class TransactionLocksClusterSharding(implicit s: TransactionLockSettings) extends PersistentActor with ActorLogging {
  val transactionLock: ActorRef = context.watch(context.actorOf(TransactionLocks.props(), TransactionLocks.name))

  println(s"\n\nTransactionLocksClusterSharding is started on ${Cluster(context.system).selfUniqueAddress}")

  override def receiveRecover: Receive = {
    case v: ReceivedMessage =>
      println(s"\n\n${self} RECEIVE RECOVER ${v}")
    case v: SnapshotOffer =>
      println(s"\n\n${self} SNAPSHOT OFFER ${v}")
  }

  override def receiveCommand: Receive = {
    case v: TransactionLocks.RequestMessage =>
      println(s"\n\n${self} RECEIVE ${v}")
      persist(ReceivedMessage(sender(), v))(r => transactionLock.tell(r.request, r.sender))
  }

  override def persistenceId: String = TransactionLocksClusterSharding.name
}
