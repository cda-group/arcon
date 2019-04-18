package common

import Identifiers._
import akka.actor.{ActorPath, Address, RootActorPath}

private object ActorPaths {
  def appMaster(member: Address): ActorPath =
    RootActorPath(member) / USER / LISTENER / APP_MASTER

  def stateMaster(member: Address): ActorPath =
    RootActorPath(member) / USER / LISTENER / STATE_MASTER
}
