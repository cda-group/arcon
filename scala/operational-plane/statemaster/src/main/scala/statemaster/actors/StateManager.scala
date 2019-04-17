package statemaster.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.metrics.ClusterMetricsExtension
import common.Identifiers
import kompact.KompactExtension

import scala.collection.mutable

/*
object StateManager {
  def apply(): Props = Props(new StateManager())
}
*/

/**
  * StateManager is a node that is responsible for creating
  * StateMasters for each App. StateMasters receive monitoring
  * stats and can act upon them.
  */

/*
class StateManager extends Actor with ActorLogging {
  private var stateMasters = mutable.IndexedSeq.empty[ActorRef]
  private var stateMasterId: Long = 0 // Unique actor id for each stateMaster that is created

  // Handles implicit conversions of ActorRef and ActorRefProto
  implicit val sys: ActorSystem = context.system
  import protobuf.ProtoConversions.ActorRef._

  private val metrics = ClusterMetricsExtension(context.system)

  override def preStart(): Unit = {
    metrics.subscribe(self)
  }

  override def postStop(): Unit = {
    metrics.unsubscribe(self)
  }

  def receive = {
    case StateManagerJob(appMaster, app) =>
      val stateMaster = context.actorOf(StateMaster(appMaster, app)
        , Identifiers.STATE_MASTER + stateMasterId)
      stateMasters = stateMasters :+ stateMaster
      stateMasterId += 1

      // Enable deathwatch
      context watch stateMaster

      // Respond with Ref to StateMaster and Kompact Proxy Addr
      val addr = KompactExtension(context.system).getProxyAddr
      sender() ! StateMasterConn(stateMaster, addr)
    case Terminated(ref) =>
      stateMasters = stateMasters.filterNot(_ == ref)
    case _ =>
  }
}
*/
