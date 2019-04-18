package kompact

import akka.actor.{ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}


// Exists in runtime-protobuf...
object ExternalAddress extends ExtensionId[ExternalAddressExt] with ExtensionIdProvider {
  override def lookup() = ExternalAddress

  override def createExtension(system: ExtendedActorSystem): ExternalAddressExt =
    new ExternalAddressExt(system)

  override def get(system: ActorSystem): ExternalAddressExt = super.get(system)
}

class ExternalAddressExt(system: ExtendedActorSystem) extends Extension {
  def addressForAkka: Address = system.provider.getDefaultAddress
}
