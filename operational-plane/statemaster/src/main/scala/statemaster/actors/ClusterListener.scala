package statemaster.actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp, UnreachableMember}
import common.Identifiers
import kompact.KompactExtension


object ClusterListener {
  def apply(): Props = Props(new ClusterListener)
}

class ClusterListener extends Actor with ActorLogging {
  private val cluster = Cluster(context.system)

  override def preStart(): Unit =
    cluster.subscribe(self, classOf[MemberUp], classOf[UnreachableMember], classOf[MemberRemoved])
  override def postStop(): Unit =
    cluster.unsubscribe(self)

  def receive = {
    case MemberUp(member) =>
    case UnreachableMember(member) =>
    case MemberRemoved(member, previousStatus) =>
  }

}
