package id.au.fsat.susan.calvin.lock

import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.Cluster
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.persistence.{ PersistentActor, SnapshotOffer }
import id.au.fsat.susan.calvin.lock.RecordLocksClusterSharding.ReceivedMessage

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
    Props(new RecordLocksClusterSharding())

  case class ReceivedMessage(sender: ActorRef, request: RecordLocks.RequestMessage)
}

class RecordLocksClusterSharding(implicit s: RecordLockSettings) extends PersistentActor with ActorLogging {
  val transactionLock: ActorRef = context.watch(context.actorOf(RecordLocks.props(), RecordLocks.name))

  println(s"\n\nTransactionLocksClusterSharding is started on ${Cluster(context.system).selfUniqueAddress}")

  override def receiveRecover: Receive = {
    case v: ReceivedMessage =>
      println(s"\n\n${self} RECEIVE RECOVER ${v}")
    case v: SnapshotOffer =>
      println(s"\n\n${self} SNAPSHOT OFFER ${v}")
  }

  override def receiveCommand: Receive = {
    case v: RecordLocks.RequestMessage =>
      println(s"\n\n${self} RECEIVE ${v}")
      persist(ReceivedMessage(sender(), v))(r => transactionLock.tell(r.request, r.sender))
  }

  override def persistenceId: String = RecordLocksClusterSharding.name
}
