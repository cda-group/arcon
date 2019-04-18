package protobuf

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer

import scalapb.GeneratedMessageCompanion


/**
  * Serializer for Proto messages in the Runtime
  * Based on https://gist.github.com/thesamet/5d0349b40d3dc92859a1a2eafba448d5
  */
class ProtobufSerializer(val system: ExtendedActorSystem) extends Serializer {
  private val classToCompanionMapRef = new AtomicReference[Map[Class[_], GeneratedMessageCompanion[_]]](Map.empty)

  override def includeManifest: Boolean = true

  // 0-40 is reserved for Akka
  override def identifier = 1313131

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case gmc: scalapb.GeneratedMessageCompanion[_] =>
      val incorrect = gmc.defaultInstance
        .toString
        .filterNot(c => c ==  '(' || c == ')')
      throw new IllegalArgumentException("Proto protobuf.messages are case classes, use " +
        gmc.defaultInstance + " instead of " + incorrect)
    case e: scalapb.GeneratedMessage => e.toByteArray
    case _ => throw new IllegalArgumentException("Need a subclass of scalapb.GeneratedMessage")
  }


  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    manifest match {
      case Some(clazz) =>
        @scala.annotation.tailrec
        def messageCompanion(companion: GeneratedMessageCompanion[_] = null): GeneratedMessageCompanion[_] = {
          val classToCompanion = classToCompanionMapRef.get()
          classToCompanion.get(clazz) match {
            case Some(cachedCompanion) => cachedCompanion
            case None =>
              val uncachedCompanion =
                if (companion eq null) Class.forName(clazz.getName + "$", true, clazz.getClassLoader)
                  .getField("MODULE$").get().asInstanceOf[GeneratedMessageCompanion[_]]
                else companion
              if (classToCompanionMapRef.compareAndSet(classToCompanion, classToCompanion.updated(clazz, uncachedCompanion)))
                uncachedCompanion
              else
                messageCompanion(uncachedCompanion)
          }
        }
        messageCompanion().parseFrom(bytes).asInstanceOf[AnyRef]
      case _ => throw new IllegalArgumentException("Need a ScalaPB companion class to be able to deserialize.")
    }
  }
}
