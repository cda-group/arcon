package appmaster

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.ActorMaterializer
import com.google.protobuf.ByteString
import se.kth.arcon.appmaster.arconc._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object Appmaster extends App {
  implicit val sys = ActorSystem("Appmaster")
  implicit val mat = ActorMaterializer()
  implicit val ec = sys.dispatcher

  val settings = GrpcClientSettings.connectToServiceAt("localhost", 3000)
    .withDeadline(250.second)
    .withTls(false)

  val client: Arconc = ArconcClient(settings)

  sys.log.info("Performing Request")
  val json = 
    """
    {
    "id": "appmaster_test",
    "target": "x86-64-unknown-linux-gnu",
    "mode" : "debug",
    "nodes": [
        {
            "id": "node_1",
            "parallelism": 1,
            "kind": {
                "Source": {
                    "source_type": {
                        "Scalar": "u32"
                    },
                    "successors": [
                        {
                            "Local": {
                                "id": "node_2"
                            }
                        }
                    ],
                    "kind": {
                        "Socket": { "host": "localhost", "port": 1337}
                    }
                }
            }
        },
        {
            "id": "node_2",
            "parallelism": 1,
            "kind": {
                "Task": {
                    "input_type": {
                        "Scalar": "u32"
                    },
                    "output_type": {
                        "Scalar": "u32"
                    },
                    "weld_code": "|x: u32| x + u32(5)",
                    "successors": [
                        {
                            "Local": {
                                "id": "node_3"
                            }
                        }
                    ],
                    "predecessor": "node_1",
                    "kind": "Map"
                }
            }
        },
        {
            "id": "node_3",
            "parallelism": 1,
            "kind": {
                "Sink": {
                    "sink_type": {
                        "Scalar": "u32"
                    },
                    "predecessor": "node_2",
                    "kind": "Debug"
                }
            }
        }
    ]
    }"""

  val req = ByteString.copyFromUtf8(json)
  val reply = client.compile(ArconcRequest(req))
  reply.onComplete {
    case Success(msg) =>
      println(s"got reply: $msg")
    case Failure(e) =>
      println(s"Error: $e")
  }


  import scala.concurrent.Await
  import scala.concurrent.duration._
  Await.ready(sys.whenTerminated, 365.days)
}
