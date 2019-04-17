package kompact

import java.nio.ByteOrder

import com.typesafe.scalalogging.LazyLogging
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.util.ReferenceCountUtil
import kompact.messages._
import kompact.netty.{KompactDecoder, KompactEncoder}


class SimpleClient extends LazyLogging {
  private val bGroup = new NioEventLoopGroup()

  def run(host: String, port: Int, testActorPath: String): Unit = {
    try {
      val bootstrap = new Bootstrap()
      bootstrap.group(bGroup)
      bootstrap.channel(classOf[NioSocketChannel])
      bootstrap.option(ChannelOption.SO_KEEPALIVE, true: java.lang.Boolean)

      bootstrap.handler(new ChannelInitializer[SocketChannel](){
        @throws[Exception]
        override def initChannel(channel: SocketChannel): Unit = {
          channel.pipeline().addLast(
            new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, Integer.MAX_VALUE, 0, 4, -4, 0, false),
            new LengthFieldPrepender(ByteOrder.BIG_ENDIAN, 4, 0, true),
            new KompactDecoder(),
            new KompactEncoder(),
            ClientHandler(testActorPath)
          )
          channel.config().setReuseAddress(true)
        }
      })
      logger.info("Connecting to server")

      val future = bootstrap.connect(host, port).sync()
      future.channel().closeFuture().sync()
    } finally {
      bGroup.shutdownGracefully()
    }
  }

  def close(): Unit = {
    bGroup.shutdownGracefully()
  }
}

final case class ClientHandler(testActorPath: String) extends ChannelInboundHandlerAdapter with LazyLogging with TestSettings {
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    try {
      val e: KompactAkkaEnvelope = msg.asInstanceOf[KompactAkkaEnvelope]
      e.msg.payload match {
        case KompactAkkaMsg.Payload.Hello(v) =>
          println(v)
        case KompactAkkaMsg.Payload.Ask(ask) =>
          val hello = Hello("ask_hello_reply")
          val askReply = AskReply(ask.askActor, KompactAkkaMsg().withHello(hello))
          // Just for testing purposes...
          val srcPath = KompactAkkaPath("random", "127.0.0.1", 0)
          val dstPath = KompactAkkaPath("random", "127.0.0.1", 0)
          val reply = KompactAkkaEnvelope(src = srcPath, dst = dstPath).
            withMsg(KompactAkkaMsg().withAskReply(askReply))
          ctx.writeAndFlush(reply)
        case _ => println("unknown")
      }
    } finally {
      ReferenceCountUtil.release(msg)
    }
  }
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info(s"Client Connected to ${ctx.channel().remoteAddress()}")
    val src = KompactAkkaPath("executor", "127.0.0.1", 2020)
    val dst = KompactAkkaPath(testActorPath, "127.0.0.1", 1337)
    val s = ExecutorRegistration(testActorPath, src, dst)
    val reg = KompactAkkaEnvelope(src, dst, msg = KompactAkkaMsg().withExecutorRegistration(s))
    ctx.writeAndFlush(reg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit =
    ctx.close()
}
