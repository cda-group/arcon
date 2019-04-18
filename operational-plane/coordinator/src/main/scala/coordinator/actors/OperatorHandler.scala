package coordinator.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import common.Identifiers
import kompact.{KompactUp, KompactTerminated, KompactExtension, KompactRef}
import kompact.messages.{Hello, KompactAkkaMsg, TaskMetric}
import protobuf.messages._

import scala.collection.mutable

object OperatorHandler {
  def apply(): Props =
    Props(new OperatorHandler())
}

class OperatorHandler extends Actor with ActorLogging {
  private var operatorRef = None: Option[KompactRef]

  // Handles implicit conversions of ActorRef and ActorRefProto
  implicit val sys: ActorSystem = context.system
  import protobuf.ProtoConversions.ActorRef._

  private val kompactExtension = KompactExtension(context.system)

  override def preStart(): Unit = {
    kompactExtension.register(self)
  }

  override def postStop(): Unit = {
    kompactExtension.unregister(self)
    kompactRefs.foreach(_.kill())
  }

  def receive = {
    case KompactAkkaMsg(payload) =>
      log.info(s"Received msg from executor $payload")
    case Terminated(ref) =>
    case KompactUp(ref) =>
      log.info(s"Kompact up ${ref.srcPath}")
      operatorRef = Some(ref)
      // Enable DeathWatch
      ref kompactWatch self
    case KompactTerminated(ref) =>
      operatorRef = None
    case _ =>
  }

}
