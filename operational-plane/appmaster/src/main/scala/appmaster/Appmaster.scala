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

object Appmaster extends App {
   if (args.length == 0) {
     println("Path to JSON file required!")
   } else {
    val filePath = args(0)

    implicit val sys = ActorSystem("Appmaster")
    implicit val mat = ActorMaterializer()
    implicit val ec = sys.dispatcher

    val settings = GrpcClientSettings.fromConfig(Arconc.name)
    val client: Arconc = ArconcClient(settings)

    val jsonInput = Source.fromFile(filePath).getLines.mkString
    sys.log.info("Sending compilation request to the Arcon compiler")
    val arconSpec = ByteString.copyFromUtf8(jsonInput)
    val request = client.compile(ArconcRequest(arconSpec))

    request.onComplete {
      case Success(reply) =>
        println(s"replied with: $reply")
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
