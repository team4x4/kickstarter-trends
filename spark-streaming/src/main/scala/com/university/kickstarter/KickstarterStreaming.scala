package com.university.kickstarter

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object KickstarterStreaming {

  private var Successful = "successful"
  private var Failed = "failed"

  case class Popularity(country: String, count: Int)
  case class Project(country: String, state: String)


  private[kickstarter] def filterByState(input: RDD[Project], state: String) = {
    require(state != null)
    input.filter(p => p != null)
      .filter(project => state.equals(project.state))
      .map(project => project.country)
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(r => Popularity(r._1, r._2))
      .sortBy(r => (-r.count, r.country), ascending = true)
  }


  def processTrendingProjects(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int, n: Int): Unit = {
    input.foreachRDD(rdd => DataConverter.saveData(rdd))
    val lineParameters = 17
    val countryIndex = 11
    val stateIndex = 9
    val projectsStream: DStream[Project] = input
      .map(line => line.split(",").map(_.trim)).filter(!_.isEmpty)
      // TODO - optimize filtering bad data
      .filter(_.length == lineParameters)
      .map(row => Project(row(countryIndex), row(stateIndex)))
      .window(Seconds(windowLength), Seconds(slidingInterval))
    projectsStream.foreachRDD(rdd => {
      DataConverter.saveData(filterByState(rdd, Successful).take(n), windowLength)
    })

    projectsStream.foreachRDD(rdd => {
      DataConverter.saveData(filterByState(rdd, Failed)
        .sortBy(r => (-r.count, r.country), ascending = true)
        .take(n), windowLength)
    })
  }
}
