import sbt._
import Keys._

import sbtassembly.AssemblyKeys._

object Assembly {

  def settings(mc: String, jarName: String) = {
    Seq(
      mainClass in assembly := Some(mc),
      assemblyJarName in assembly := jarName,
      test in assembly := {}
    )
  }

}
