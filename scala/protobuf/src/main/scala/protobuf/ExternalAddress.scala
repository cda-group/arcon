package protobuf

import akka.actor.{ActorRef, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}


class ExternalAddressExt(system: ExtendedActorSystem) extends Extension {
  def addressForAkka: Address = system.provider.getDefaultAddress
  def resolveRef(path: String): ActorRef = system.provider.resolveActorRef(path)
}


object ExternalAddress extends ExtensionId[ExternalAddressExt] with ExtensionIdProvider {
  override def lookup() = ExternalAddress

  override def createExtension(system: ExtendedActorSystem): ExternalAddressExt =
    new ExternalAddressExt(system)

  override def get(system: ActorSystem): ExternalAddressExt = super.get(system)
}
