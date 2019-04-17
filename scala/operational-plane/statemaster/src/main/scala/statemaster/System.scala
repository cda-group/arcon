package statemaster

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import common.Identifiers
import actors.ClusterListener
import kompact.KompactExtension
import utils.Config

object System extends App with Config with LazyLogging {
  logger.info("Starting up Statemaster")
  val system = ActorSystem(Identifiers.CLUSTER, config)
  val handler = system.actorOf(ClusterListener(), Identifiers.LISTENER)

  system.whenTerminated
}
