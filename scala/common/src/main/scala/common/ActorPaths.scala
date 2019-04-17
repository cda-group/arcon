package common

import Identifiers._
import akka.actor.{ActorPath, Address, RootActorPath}

private object ActorPaths {
  def appManager(member: Address): ActorPath =
    RootActorPath(member) / USER / LISTENER / APP_MANAGER

  def stateManager(member: Address): ActorPath =
    RootActorPath(member) / USER / LISTENER / STATE_MANAGER
}
