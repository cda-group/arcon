package statemaster.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import common.Identifiers
import kompact.{ExecutorTerminated, ExecutorUp, KompactExtension, KompactRef}
import kompact.messages.{Hello, KompactAkkaMsg}
import protobuf.messages._

import scala.collection.mutable

object StateMaster {
  def apply(appMaster: ActorRef, app: ArcApp): Props =
    Props(new StateMaster(appMaster, app))
}

class StateMaster(appMaster: ActorRef, app: ArcApp) extends Actor with ActorLogging {
  private var metricMap = mutable.HashMap[ArcTask, ExecutorMetric]()
  private var kompactRefs = IndexedSeq.empty[KompactRef]

  // Handles implicit conversions of ActorRef and ActorRefProto
  implicit val sys: ActorSystem = context.system
  import protobuf.ProtoConversions.ActorRef._

  private val kompactExtension = KompactExtension(context.system)

  override def preStart(): Unit = {
    context watch appMaster
    kompactExtension.register(self)
  }

  override def postStop(): Unit = {
    kompactExtension.unregister(self)
    kompactRefs.foreach(_.kill())
  }

  def receive = {
    case ArcTaskMetric(task, metric) =>
      metricMap.put(task, metric)
    case ArcAppMetricRequest(id) if app.id.equals(id) =>
      val report = ArcAppMetricReport(id, metricMap.map(m => ArcTaskMetric(m._1, m._2)).toSeq)
      sender() ! report
    case ArcAppMetricRequest(_) =>
      sender() ! ArcAppMetricFailure("App ID did not match up")
    case KompactAkkaMsg(payload) =>
      log.info(s"Received msg from executor $payload")
    case Terminated(ref) =>
      // AppMaster has been terminated
      // Handle
      // context stop self
    case ExecutorUp(ref) =>
      log.info(s"Kompact Executor up ${ref.srcPath}")
      kompactRefs = kompactRefs :+ ref
      // Enable DeathWatch
      ref kompactWatch self
      val hello = Hello("Akka saying hello from statemaster")
      val welcomeMsg = KompactAkkaMsg().withHello(hello)
      ref ! welcomeMsg
    case ExecutorTerminated(ref) =>
      kompactRefs = kompactRefs.filterNot(_ == ref)
    case _ =>
  }

}
