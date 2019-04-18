package kompact.serialization


object Frame {
  final val frameType = 0x02
  final val magicNumber = 0xC0A1BA11
  // // (frame length) + (magic # length) + (frame type)
  final val headerLen = 4 + 4 + 1
}

object AddressType {
  val IPV4 = 0
  val IPV6 = 1
  val DOMAIN = 2
}

object Protocol {
  val LOCAL = 0
  val TCP = 1
  val UDP = 2
}

object PathType {
  val UniquePath = 0
  val NamedPath = 1
}

