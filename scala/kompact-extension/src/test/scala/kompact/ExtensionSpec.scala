package kompact

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import kompact.ExtensionActor.TestAsk
import kompact.KompactAsk.AskSuccess
import kompact.messages.{Hello, KompactAkkaEnvelope, KompactAkkaMsg}

import scala.concurrent.{ExecutionContext, Future}

object ExtensionSpec {
  val actorSystem = ActorSystem("ExtensionSpec", ConfigFactory.parseString(
    """
      | akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.stdout-loglevel = "OFF"
      | akka.loglevel = "OFF"
      | akka.extensions = ["kompact.KompactExtension"]
      | akka.kompact.port = 1337
      | akka.kompact.host = "127.0.0.1"
      | akka.log-dead-letters-during-shutdown = false
      | akka.log-dead-letters = 0
    """.stripMargin))

}

class ExtensionSpec extends TestKit(ExtensionSpec.actorSystem)
  with ImplicitSender with ActorSpec with TestSettings {

  implicit val ec: ExecutionContext = system.dispatcher

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "KompactExtension" must {

    "Register executor, perform ask request and handle DeathWatch" in {
      val extensionActor = system.actorOf(ExtensionActor(self), extensionActorName)
      val formattedPath = extensionActor.path.toSerializationFormatWithAddress(ExternalAddress(system).addressForAkka)
      val port = system.settings.config.getInt("akka.kompact.port")
      val host = system.settings.config.getString("akka.kompact.host")
      val simpleClient = new SimpleClient()
      Future(simpleClient.run(host, port, formattedPath))
      // We should receive an ExecutorUp msg when a client connects
      expectMsgType[ExecutorUp]

      val msg = KompactAkkaMsg().withHello(Hello("ask"))
      extensionActor ! TestAsk(msg)
      expectMsgType[KompactAkkaMsg]

      import ExtensionActor._
      extensionActor ! TestDeathWatch
      expectMsgType[ExecutorTerminated]
    }
  }

}
