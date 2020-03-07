name := "kickstarter-trends"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0"
)