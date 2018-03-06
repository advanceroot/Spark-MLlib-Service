name := "OnlinePredictor"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "com.typesafe.akka" %% "akka-actor" % "2.3.16",
  "com.typesafe.akka" %% "akka-remote" % "2.3.16",
  "io.smls.base" %% "interface" % "0.1-SNAPSHOT"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
