package statemaster.utils

import com.typesafe.config.{ConfigFactory, ConfigList}
import common.Identifiers

trait Config {
  val config = ConfigFactory.load()

  val roles: ConfigList = config.getList("akka.cluster.roles")
  require(roles.unwrapped().contains(Identifiers.STATE_MASTER),
    "StateMaster role has not been set")
}
