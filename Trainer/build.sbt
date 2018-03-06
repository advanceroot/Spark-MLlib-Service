name := "Trainer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
  "io.smls.base" %% "interface" % "0.1-SNAPSHOT"
)

//exclude scala library
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)