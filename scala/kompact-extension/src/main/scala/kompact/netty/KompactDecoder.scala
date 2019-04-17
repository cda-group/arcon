package kompact.netty

import java.nio.ByteBuffer
import java.util

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import kompact.messages.{KompactAkkaEnvelope, KompactAkkaPath}
import java.nio.charset.StandardCharsets
import java.util.UUID

private[kompact] class KompactDecoder extends MessageToMessageDecoder[ByteBuf] {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    try {
      val frameLen = in.readInt()
      val magicNum = in.readInt()
      val frameTyp = in.readByte()
      val streamId = in.readInt()
      val seqNum = in.readInt()
      val payloadLen = in.readInt()
      val (src, _buf) = deserializeActorpath(in)
      val (dest, buf) = deserializeActorpath(_buf)
      val serIdent = buf.readLong()

      val length = buf.readableBytes()
      val actualMsg = buf.readBytes(length)
      val array = ByteBufUtil.getBytes(actualMsg)

      val msg = KompactAkkaEnvelope.parseFrom(array)
      out.add(msg)
    } catch {
      case err: Exception =>
        println(err.toString)
    }
  }

  private def deserializeActorpath(buf: ByteBuf): (KompactAkkaPath, ByteBuf) = {
    val field = buf.readByte()
    val ip1 = buf.readByte()
    val ip2 = buf.readByte()
    val ip3 = buf.readByte()
    val ip4 = buf.readByte()
    val ipAddr = Array[Byte](ip1, ip2, ip3, ip4).map(_.toInt).mkString(".")

    val port = buf.readUnsignedShort

    val isNamedPath = (field & (1 << 7)) != 0

    if (isNamedPath) {
      val namedLength = buf.readUnsignedShort()
      val target = buf.readBytes(namedLength)
      val targetStr = target.toString(StandardCharsets.UTF_8)
      (KompactAkkaPath(targetStr, ipAddr, port), buf)
    } else {
      val data = buf.readBytes(16)
      val buffer = ByteBuffer.wrap(ByteBufUtil.getBytes(data))
      val uuid = new UUID(buffer.getLong, buffer.getLong())
      (KompactAkkaPath(uuid.toString, ipAddr, port), buf)
    }
  }

}
