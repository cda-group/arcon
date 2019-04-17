package kompact.serialization


// https://github.com/kompics/kompact/blob/master/core/src/messaging/framing.rs
trait SerIdents {
  final val ProtoSerIdent: Long = 20
  final val DispatchEnvelope = 0x01
  final val UniqueActorPath = 0x02
  final val NamedActorPath = 0x03
  final val SystemPathHeader = 0x04
  final val SystemPath = 0x05
}
