package com.university.kickstarter

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object KickstarterStreaming {

  private var Successful = "success"
  private var Failed = "failed"

  case class Popularity(country: String, count: Int)
  case class Project(country: String, state: String)


  def processTrendingProjects(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int, n: Int): Unit = {
    //input.foreachRDD(rdd => DataConverter.saveData(rdd))
    val lineParameters = 17
    val countryIndex = 11
    val stateIndex = 9
    val projectsStream: DStream[Project] = input
      .map(line => line.split(",").map(_.trim)).filter(!_.isEmpty)
      .filter(_.length == lineParameters)
      .map(row => Project(row(countryIndex), row(stateIndex)))
      .window(Seconds(windowLength), Seconds(slidingInterval))

    projectsStream.foreachRDD(rdd => {

      val successfulProjects = rdd.filter(p => p != null)
        .filter(project => Successful.equals(project.state))
        .map(project => project.country)
        .map((_, 1))
        .reduceByKey(_ + _)
        .map(r => Popularity(r._1, r._2))
        .sortBy(r => (-r.count, r.country), ascending = true)

      val countSuc = successfulProjects.map(_.count).sum().toInt

      val failedProjects = rdd.filter(p => p != null)
        .filter(project => Failed.equals(project.state))
        .map(project => project.country)
        .map((_, 1))
        .reduceByKey(_ + _)
        .map(r => Popularity(r._1, r._2))
        .sortBy(r => (-r.count, r.country), ascending = true)

      val countFailed = successfulProjects.map(_.count).sum().toInt

      DataConverter.saveData(successfulProjects.take(n), countSuc,
        failedProjects.take(n), countFailed, rdd.name, windowLength)
    })
  }
}
