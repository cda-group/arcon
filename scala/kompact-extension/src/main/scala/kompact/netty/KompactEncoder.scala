package kompact.netty


import com.typesafe.scalalogging.LazyLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import kompact.messages.{KompactAkkaEnvelope, KompactAkkaPath}
import kompact.serialization.{Frame, SerIdents}


private[kompact] class KompactEncoder extends MessageToByteEncoder[KompactAkkaEnvelope] with SerIdents with LazyLogging {
  private var seqNum = 0

  override def encode(ctx: ChannelHandlerContext, msg: KompactAkkaEnvelope, outBuf: ByteBuf): Unit = {
    try {
      val streamId = 1 // ???
      val payloadLen = sizeOfNamedPath(msg.src) + sizeOfNamedPath(msg.dst) + 8 + msg.serializedSize

      outBuf.writeInt(Frame.magicNumber)
      outBuf.writeByte(Frame.frameType.toByte)
      outBuf.writeInt(streamId)
      outBuf.writeInt(seqNum)
      outBuf.writeInt(payloadLen)

      serializeActorpath(msg.src, outBuf)
      serializeActorpath(msg.dst, outBuf)
      outBuf.writeLong(ProtoSerIdent)
      outBuf.writeBytes(msg.toByteArray)
    } catch {
      case err: Exception =>
        logger.error(err.toString)
    }
  }

  private def sizeOfNamedPath(p:KompactAkkaPath): Int = {
    1 + 4 + 2 + 2 + p.path.length
  }

  private def serializeActorpath(p: KompactAkkaPath, out: ByteBuf): Unit = {
    // Fields 1 byte
    // Ip 4 bytes
    // port 2 bytes
    // namedLength 2 bytes
    // namedPath -> N bytes (.length)

    // For now, sets as Named and TCP
    val field = 129.toByte

    out.writeByte(field)
    val ips: Array[Byte] = p.ip.split('.').map(_.toByte)
    out.writeBytes(ips)

    out.writeShort(p.port)
    out.writeShort(p.path.length)
    out.writeBytes(p.path.getBytes)

  }
}

