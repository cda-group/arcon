import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

name := "arcon." + "root"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked"
)

lazy val asciiGraphs = RootProject(uri("git://github.com/Max-Meldrum/ascii-graphs.git"))
version in asciiGraphs := "0.0.7-SNAPSHOT"

lazy val arc = RootProject(uri("git://github.com/cda-group/arc.git"))
version in arc := "0.1.0-SNAPSHOT"

lazy val compilerPipeline = RootProject(uri("git://github.com/cda-group/compiler-pipeline.git"))
version in arc := "0.1.0-SNAPSHOT"

lazy val generalSettings = Seq(
  organization := "se.kth.arcon",
  scalaVersion := "2.12.8"
)

lazy val arconSettings = generalSettings ++ Seq(
  fork in run := true,  // https://github.com/sbt/sbt/issues/3736#issuecomment-349993007
  cancelable in Global := true,
  version := "0.1",
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  fork in Test := true
)

lazy val root = (project in file("."))
  .aggregate(kompactExtension, asciiGraphs, arc, compilerPipeline,
      common, protobuf, statemaster, appmaster)

lazy val kompactExtension = (project in file("kompact-extension"))
  .settings(arconSettings: _*)
  .settings(Dependencies.kompactExtension)
  .settings(moduleName("kompact"))
  .settings(
    PB.protoSources in Compile := Seq(file("../proto"))
  )
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val protobuf = (project in file("protobuf"))
  .settings(arconSettings: _*)
  .settings(Dependencies.protobuf)
  .settings(moduleName("protobuf"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val common = (project in file("common"))
  .settings(arconSettings: _*)
  .settings(Dependencies.common)
  .settings(moduleName("common"))

lazy val statemaster = (project in file("statemaster"))
  .dependsOn(protobuf, common, kompactExtension % "test->test; compile->compile")
  .settings(arconSettings: _*)
  .settings(Dependencies.statemaster)
  .settings(moduleName("statemaster"))
  .settings(Assembly.settings("statemaster.System", "statemaster.jar"))
  .settings(Sigar.loader())

lazy val appmaster = (project in file("appmaster"))
  .dependsOn(compilerPipeline, common % "test->test; compile->compile")
  .settings(arconSettings: _*)
  .settings(Dependencies.appmaster)
  .settings(moduleName("appmaster"))
  .settings(Assembly.settings("appmaster.Appmaster", "appmaster.jar"))
  .settings(
    javaAgents += "org.mortbay.jetty.alpn" % "jetty-alpn-agent" % "2.0.9" % "runtime;test"
  )
  .enablePlugins(JavaAgent)
  .enablePlugins(AkkaGrpcPlugin)
  .settings(
    PB.protoSources in Compile := Seq(file("../proto/"))
  )


lazy val coordinator = (project in file("coordinator"))
  .dependsOn(protobuf, common, kompactExtension % "test->test; compile->compile")
  .settings(arconSettings: _*)
  .settings(Dependencies.coordinator)
  .settings(moduleName("coordinator"))
  .settings(Assembly.settings("coordinator.System", "coordinator.jar"))
  .settings(Sigar.loader())

def moduleName(m: String): Def.SettingsDefinition = {
  val mn = "Module"
  packageOptions in (Compile, packageBin) += Package.ManifestAttributes(mn → m)
}
