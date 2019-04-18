package kompact

import akka.actor.{Actor, ActorRef, ActorSystem, Status}
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.{Future => JFuture}

import akka.util.Timeout
import kompact.KompactAsk.{AskResponse, AskSuccess, AskTickerInit}
import kompact.messages._

import scala.util.{Failure, Success}


object KompactApi {
  implicit def pipez[T](future: Future[T])(implicit executionContext: ExecutionContext): KompactPipe[T] =
    new KompactPipe[T](future)

  final class KompactPipe[T](f: Future[T])(implicit ec: ExecutionContext) {
    def pipeKompactTo(target: ActorRef)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
      f map {
        case AskSuccess(msg) =>
          target ! msg
        case _=>
          target ! Status.Failure(new Throwable("Kompact Pipe Failure"))
      }
      f
    }

    def pipeToKompact(target: KompactRef): Future[T] = {
      f onComplete {
        case Success(v) =>
          v match {
            case msg@KompactAkkaMsg(_) =>
              target ! msg
            case AskSuccess(msg) =>
              target ! msg
            case _ =>
              Future.failed(new Throwable("Expected a type of KompactMsg"))
          }
        case Failure(e) =>
          Future.failed(new Throwable("Kompact Pipe Failure"))
      }
      f
    }
  }

}

/** Commands that can be executed on KompactRef's
  */
private[kompact] trait KompactApi {


  /** Fire and forget message sending
    * @param payload Protobuf Message
    */
  def !(payload: KompactAkkaMsg): Unit


  /** Returns a Future awaiting a response from the executor
    * @param payload Protobuf Message
    * @return Future[AskResponse] i.e., AskSuccess/AskFailure
    */
  def ?(payload: KompactAkkaMsg)(implicit sys: ActorSystem,
                             ec: ExecutionContext,
                             t: Timeout): Future[AskResponse]


  /** Enable Akka Actors to DeathWatch KompactRef's
    * @param watcher ActorRef
    */
  def kompactWatch(watcher: ActorRef): Unit

  /** Close a connection to an Executor
    */
  def kill(): Unit

}


/** KompactRef represents a Kompact connection
  * @param jobId  String
  * @param srcPath KompactAkkaPath Object
  * @param dstPath KompactAkkaPath Object
  * @param ctx Netty ContextHandlerContext
  */
final case class KompactRef(jobId: String,
                            srcPath: KompactAkkaPath,
                            dstPath: KompactAkkaPath,
                            ctx: ChannelHandlerContext
                           ) extends KompactApi {


  override def !(payload: KompactAkkaMsg): Unit = {
    if (ctx.channel().isWritable) {
      val envelope = KompactAkkaEnvelope(src = Some(dstPath), dst = Some(srcPath), msg = Some(payload))
      ctx.writeAndFlush(envelope)
    }
  }

  override def ?(msg: KompactAkkaMsg)(implicit sys: ActorSystem,
                                      ec: ExecutionContext,
                                      t: Timeout): Future[AskResponse] = {
    val askActor = sys.actorOf(KompactAsk(t))
    // This might have to be toStringWithAddress..
    val _ask = Ask(askActor.path.toString, Some(msg))
    val askReq = KompactAkkaMsg().withAsk(_ask)
    val envelope = KompactAkkaEnvelope(src = Some(dstPath), dst = Some(srcPath), msg = Some(askReq))
    ctx.writeAndFlush(envelope)
    import akka.pattern._
    askActor.ask(AskTickerInit).mapTo[AskResponse]
  }

  override def kompactWatch(watcher: ActorRef): Unit =  {
    ctx.channel().closeFuture().addListener((_: JFuture[Void]) => {
      // Channel has been closed
      watcher ! KompactTerminated(this)
    })
  }

  override def kill(): Unit = ctx.close()
}
