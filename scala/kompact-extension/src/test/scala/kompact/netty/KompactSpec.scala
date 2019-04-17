package kompact.netty

import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.FlatSpec
import kompact.messages.{ExecutorRegistration, KompactAkkaEnvelope, KompactAkkaMsg, KompactAkkaPath}

class KompactSpec extends FlatSpec {

  "Kompact encoder and decoder" should "actually work" in {
    val channel = new EmbeddedChannel(new KompactEncoder, new KompactDecoder)
    val src = KompactAkkaPath("executor", "127.0.0.1", 2000)
    val dst = KompactAkkaPath("executor", "127.0.0.1", 2020)
    val s = ExecutorRegistration("test", src, dst)
    val msg = KompactAkkaEnvelope(src, dst, msg = KompactAkkaMsg().withExecutorRegistration(s))

    channel.writeOneInbound(msg)
    val obj: KompactAkkaEnvelope = channel.readInbound()
    assert(obj.msg.payload.isExecutorRegistration)
  }
}
