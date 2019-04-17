package kompact

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kompact.messages.AskReply
import kompact.netty.ProxyServer

import scala.collection.mutable


private[kompact] object ProxyActor {
  def apply(): Props = Props(new ProxyActor())
  type ActorName = String
  final case class AskRelay(reply: AskReply)
}

private[kompact] class ProxyActor extends Actor with ActorLogging {
  import KompactExtensionImpl._
  import ProxyActor._

  private val port = context.system.settings.config.getInt("akka.kompact.port")
  private val host = context.system.settings.config.getString("akka.kompact.host")
  private val selfAddr = new InetSocketAddress(host, port)

  private val proxyServer = new ProxyServer(self)

  // Store of current Akka actors
  private val refs = mutable.HashMap.empty[ActorName, ActorRef]
  // ExecutionContext for futures incl proxyServer.run
  private implicit val ec = context.system.dispatcher

  override def preStart(): Unit = {
    log.info(s"Starting up Kompact Extension at $host:$port")
    proxyServer.run(selfAddr)
  }

  override def postStop(): Unit = proxyServer.close()

  def receive = {
    case Register(ref) =>
      val formattedPath = ref.path.toSerializationFormatWithAddress(ExternalAddress(context.system).addressForAkka)
      log.info(s"Registered: $formattedPath")
      refs.put(formattedPath, ref)
    case Unregister(ref) =>
      val formattedPath = ref.path.toSerializationFormatWithAddress(ExternalAddress(context.system).addressForAkka)
      refs.remove(formattedPath)
    case msg@ExecutorUp(ref) =>
      refs.get(ref.dstPath.path) match {
        case Some(akkaRef) =>
          akkaRef ! msg // Send KompactRef to Akka Actor
          sender() ! akkaRef // Send ActorRef to handler responsible for akkaRef
        case None =>
          log.error(s"ProxyActor could not find actor for path ${ref.dstPath.path}")
      }
    case AskRelay(reply) =>
      val actor = context.actorSelection(reply.askActor)
      actor ! reply.msg
    case ProxyServerTerminated =>
      log.error("ProxyServer was terminated")
      // Shut down extension or simply restart it?
  }

}
