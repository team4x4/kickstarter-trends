name := "kickstarter-trends"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0",
  "com.google.guava" % "guava" % "27.1-jre",
  "com.google.cloud" % "google-cloud-storage" % "1.70.0",
//BQ samples as of 27feb2019 use hadoop2 but hadoop3 seems to work fine and are recommended elsewhere
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "hadoop3-0.13.16" % "provided",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-1.9.16" % "provided"
)