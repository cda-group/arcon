import sbt._
import Keys._

object Sigar {
  def loader() = {
    Seq(
      javaOptions in Test += s"-Djava.library.path=native/"
      //javaOptions in run += s"-Djava.library.path=native/"
    )
  }
}
