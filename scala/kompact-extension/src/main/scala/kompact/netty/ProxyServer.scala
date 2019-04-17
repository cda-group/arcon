package kompact.netty

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import kompact.ProxyServerTerminated

import scala.concurrent.{ExecutionContext, Future}

/** ProxyServer starts a netty TCP server that
  * listents to connections from Executors
  * @param proxyActor ActorRef
  */
private[kompact] class ProxyServer(proxyActor: ActorRef) extends LazyLogging {
  // TODO: Add Native Transport as option
  // Look into how many threads boss and worker group should have each
  private val bossGroup = new NioEventLoopGroup
  private val workerGroup = new NioEventLoopGroup

  private val setup = new ServerBootstrap()
    .group(bossGroup, workerGroup)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline().addLast(
          new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, Integer.MAX_VALUE, 0, 4, -4, 0, false),
          new LengthFieldPrepender(ByteOrder.BIG_ENDIAN, 4, 0, true),
          new KompactDecoder(),
          new KompactEncoder(),
          new ProxyServerHandler(proxyActor, workerGroup)
        )
        ch.config().setReuseAddress(true)
      }
    })


  def run(inet: InetSocketAddress)(implicit ec: ExecutionContext): Future[Unit] =  Future {
    try {
      // Bind and start accepting connections
      val ch: ChannelFuture = setup.bind(inet).sync()

      // Blocks until channel is closed
      ch.channel().closeFuture().sync()
    } catch {
      case err: Exception =>
        logger.error(err.toString)
        // Clean up
        close()
    }
  }


  def close(): Unit = {
    bossGroup.shutdownGracefully().await(2000)
    workerGroup.shutdownGracefully().await(2000)
    proxyActor ! ProxyServerTerminated
  }

}
