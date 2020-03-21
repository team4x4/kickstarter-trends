package com.university.kickstarter

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KickstarterTrending {
  def createStream(projectId: String, ssc : StreamingContext, windowLength: String,
                   slidingInterval: String, checkpointDirectory: String)
  : DStream[String] = {
    val subscription = "project-sub"
    val stream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectId,
        None,
        subscription,
        SparkGCPCredentials.builder.build(),
        StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    stream
  }


  def createContext(projectId: String, windowLength: String, slidingInterval: String, checkpointDirectory: String)
  : StreamingContext = {

    val sparkConf = new SparkConf().setAppName(KickstarterTrending.getClass.getName)
    sparkConf.setIfMissing("spark.master", "local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))

    val yarnTags = sparkConf.get("spark.yarn.tags")
    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    ssc.checkpoint(checkpointDirectory + '/' + jobId)

    val projectsStream: DStream[String] =
      createStream(projectId, ssc, windowLength, slidingInterval, checkpointDirectory)

    KickstarterStreaming.processTrendingProjects(projectsStream,
      windowLength.toInt,
      slidingInterval.toInt,
      10,
      projectId)
    ssc
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
           FORMAT:: <projectId> <windowLength> <slidingInterval> <totalRunningTime> <checkPointDirectory>
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectId, windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(projectId, windowLength, slidingInterval, checkpointDirectory))
    ssc.sparkContext.hadoopConfiguration
      .set("fs.file.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem].getName)
    ssc.sparkContext.hadoopConfiguration
      .set("fs.AbstractFileSystem.gs.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS].getName)
    ssc.start()
    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }
}
