addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

// Protobuf support
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.18")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.4"

// Akka gRPC
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.7.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.4") // ALPN agent

