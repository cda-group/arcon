package kompact

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import kompact.messages.KompactAkkaMsg

private[kompact] object ExtensionActor {
  def apply(parent: ActorRef): Props = Props(new ExtensionActor(parent))
  final case class TestAsk(msg: KompactAkkaMsg)
  final case object TestDeathWatch
}

private[kompact] class ExtensionActor(parent: ActorRef) extends Actor with ActorLogging {
  import ExtensionActor._

  import scala.concurrent.duration._
  import akka.pattern._

  private var kompactRef: Option[KompactRef] = None

  override def preStart(): Unit =
    KompactExtension(context.system).register(self)

  override def postStop(): Unit =
    KompactExtension(context.system).unregister(self)

  implicit val sys = context.system
  implicit val ec = context.system.dispatcher
  implicit val timeout = Timeout(3.seconds)

  def receive = {
    case msg@ExecutorUp(ref) =>
      kompactRef = Some(ref)
      parent ! msg
      ref kompactWatch parent // If killed, parent will be notified
    case TestAsk(msg) =>
      kompactRef match {
        case Some(ref) =>
          import KompactApi._
          (ref ? msg) pipeKompactTo  parent
        case None =>
      }
    case TestDeathWatch =>
      kompactRef.foreach(_.kill())
  }

}
