package coordinator.util

import com.typesafe.config.{ConfigFactory, ConfigList}
import common.Identifiers

trait Config {
  val config = ConfigFactory.load()

  val roles: ConfigList = config.getList("akka.cluster.roles")
  require(roles.unwrapped().contains(Identifiers.COORDINATOR),
    "Coordinator role has not been set")
}
