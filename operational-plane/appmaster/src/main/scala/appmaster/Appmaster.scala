package appmaster

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.ActorMaterializer
import com.google.protobuf.ByteString
import se.kth.arcon.appmaster.arconc._
import scala.io.Source

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

import se.kth.cda.compiler.Compiler._
import appmaster.util.Ascii._

object Appmaster extends App {
  if (args.length == 0) {
    println("Path to Arcon Specification required!")
  } else {
    val filePath = args(0)

    implicit val sys = ActorSystem("Appmaster")
    implicit val mat = ActorMaterializer()
    implicit val ec = sys.dispatcher

    displayHeader()

    val settings = GrpcClientSettings.fromConfig(Arconc.name)
    val client: Arconc = ArconcClient(settings)

    val jsonInput = Source.fromFile(filePath).getLines.mkString
    // Compile the arc IR to an Arcon Specification
    val result = compile(jsonInput)
    val arconSpec = ByteString.copyFromUtf8(result)
    val request = client.compile(ArconcRequest(arconSpec))
    sys.log.info("Sent request to the Arcon compiler, please hold!")

    import ArconcReply.Msg
    request.onComplete {
      case Success(reply) =>
        reply.msg match {
          case Msg.Error(err) =>
            sys.log.error(s"Failed with err ${err.message}")
          case Msg.Success(msg) =>
            sys.log.info(s"Compiled binary to path: ${msg.path}")
          case Msg.Empty =>
            sys.log.error("Received an empty reply from arconc!")
        }
        sys.terminate()
      case Failure(e) =>
        println(s"Failed to contact arconc with err: $e")
        sys.terminate()
    }

    import scala.concurrent.Await
    import scala.concurrent.duration._
    Await.ready(sys.whenTerminated, 365.days)
   }
}
