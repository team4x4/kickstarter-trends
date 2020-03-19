package com.university.kickstarter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import scala.util.Try


object KickstarterStreaming {

  private var Successful = "successful"
  private var Failed = "failed"

  case class Popularity(country: String, count: Int, time_stamp: String)

  case class KickstarterProject(id: String, name: String, category: String,
                                main_category: String,currency: String,
                                deadline: String, goal: String, launched: String,
                                pledged: String, state: String,
                                backers: String, country: String, usdPledged: String)

  object KickstarterProject {
    // ПРИМЕЧАНИЕ
    // После этого метода все данные становятся None
    // Т.к. столбцов данных постоянно то 14, то 15
    // Может, можно как-то взять ПЕРВЫЕ 13 столбцов?
    def fromList(list: Array[String]): Option[KickstarterProject] = list match {
      case Array(id, name, category, main_category,
      currency, deadline, goal, launched, pledged,
      state, backers, country, usdPledged) =>
        Try(KickstarterProject(id, name, category, main_category,
          currency, deadline, goal, launched, pledged,
          state, backers, country, usdPledged)).toOption
      case _ => None
    }
  }


  def processTrendingProjects(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int, n: Int): Unit = {

    val projectsStream: DStream[KickstarterProject] = input
      .map(line => line.split(",").map(_.trim).filter(!_.isEmpty))
         .map(row => KickstarterProject.fromList(row))
          .filter(_.isDefined).map(_.get)
          .window(Seconds(windowLength), Seconds(slidingInterval))

    projectsStream.foreachRDD(rdd => {

      //println(rdd.isEmpty())
      //rdd.foreach(elem => print(elem + " "))
      //println("jewgdajhgasjfkja")
      DataConverter.saveDataToGS(rdd)

      val time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
            val successfulProjects = rdd.filter(p => p != null)
              .filter(project => Successful.equals(project.state))
              .map(project => project.country)
              .map((_, 1))
              .reduceByKey(_ + _)
              .map(r => Popularity(r._1, r._2, time))
              .sortBy(r => (-r.count, r.country), ascending = true)


            val countSuc = successfulProjects.map(_.count).sum().toInt

            val failedProjects = rdd.filter(p => p != null)
              .filter(project => Failed.equals(project.state))
              .map(project => project.country)
              .map((_, 1))
              .reduceByKey(_ + _)
              .map(r => Popularity(r._1, r._2, time))
              .sortBy(r => (-r.count, r.country), ascending = true)

            val countFailed = failedProjects.map(_.count).sum().toInt

            DataConverter.saveData(successfulProjects.take(n), countSuc,
              failedProjects.take(n), countFailed, time,  rdd.name, windowLength)

    })
  }
}
