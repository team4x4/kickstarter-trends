package com.university.kickstarter

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.{KinesisInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KickstarterTrending {

  def createStream(ssc : StreamingContext, windowLength: String, slidingInterval: String, checkpointDirectory: String)
  : DStream[String] = {

    val kinesisConf = ConfigFactory.load.getConfig("kinesis")
    val appName = kinesisConf.getString("appName")
    val streamName = kinesisConf.getString("streamName")
    val endpointUrl = kinesisConf.getString("endpointUrl")
    val regionName = kinesisConf.getString("regionName")

    val builder = AmazonKinesisClientBuilder.standard.withCredentials(new DefaultAWSCredentialsProviderChain)
    builder.setEndpointConfiguration(new EndpointConfiguration(endpointUrl, regionName))
    val kinesisClient = builder.build

    val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards.size

    val batchInterval = Seconds(slidingInterval.toInt)
    val kinesisCheckpointInterval = batchInterval

    val kinesisStreams = (0 until numShards).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .streamName(streamName)
        //.initialPosition(InitialPositionInStream.LATEST)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build().map(x => new String(x))
    }

    ssc.union(kinesisStreams)
  }


  def createContext(windowLength: String, slidingInterval: String, checkpointDirectory: String)
  : StreamingContext = {

    val sparkConf = new SparkConf().setAppName(KickstarterTrending.getClass.getName)
    sparkConf.setIfMissing("spark.master", "local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))

    val yarnTags = sparkConf.get("spark.yarn.tags")
    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    ssc.checkpoint(checkpointDirectory + '/' + jobId)

    val projectsStream: DStream[String] = createStream(ssc, windowLength, slidingInterval, checkpointDirectory)

    KickstarterProjectsStreaming.processTrendingProjects(projectsStream,
      windowLength.toInt,
      slidingInterval.toInt,
      10)

    ssc
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println(
        """
          | Usage: KickstarterTrending <windowLength> <slidingInterval> <totalRunningTime>
          |
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |     <checkpointDirectory>: Directory used to store RDD checkpoint data
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(windowLength, slidingInterval, checkpointDirectory))

    ssc.start()
    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }
}
