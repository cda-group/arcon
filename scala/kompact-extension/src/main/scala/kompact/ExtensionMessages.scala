package kompact


final case class ExecutorTerminated(ref: KompactRef)
final case class ExecutorUp(ref: KompactRef)
final case object ProxyServerTerminated
