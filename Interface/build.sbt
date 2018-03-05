name := "Interface"

organization := "io.smls.base"

version := "0.1-SNAPSHOT"

//spark 2.1
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
)

//exclude scala library
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)