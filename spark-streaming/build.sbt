name := "kickstarter-trends"

version := "0.1"
scalaVersion := "2.11.2"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0",
  
  "com.google.guava" % "guava" % "27.1-jre",
  "com.google.cloud" % "google-cloud-storage" % "1.70.0",

  "com.google.guava" % "guava" % "27.1-jre",
  "com.google.cloud" % "google-cloud-storage" % "1.70.0",

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.12.0-beta",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "hadoop3-0.13.16" % "provided",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.17"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)