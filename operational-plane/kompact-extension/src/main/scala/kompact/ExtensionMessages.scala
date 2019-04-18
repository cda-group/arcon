package kompact

final case class KompactTerminated(ref: KompactRef)
final case class KompactUp(ref: KompactRef)
final case object ProxyServerTerminated
