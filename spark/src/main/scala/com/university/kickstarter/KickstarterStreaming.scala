package com.university.kickstarter

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime, ZoneOffset}

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import scala.util.Try


// Подсчет статистик из потока
object KickstarterStreaming {

  private var Successful = "successful"
  private var Failed = "failed"

  // Для метрик
  case class Popularity(country: String, count: Int, time_stamp: String)

  // Для сохранения на диск
  case class KickstarterProject(id: String, name: String, category: String,
                                main_category: String, currency: String,
                                deadline: String, goal: String, launched: String,
                                pledged: String, state: String,
                                backers: String, country: String, usdPledged: String)

  // Создание из массива объекта KickstarterProject
  object KickstarterProject {
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

  // Подсчет метрик
  def processTrendingProjects(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int, n: Int, projectId : String): Unit = {

    // Сначала фильтруем входной поток
    val projectsStream: DStream[KickstarterProject] = input
      .map(line => line.split(",").map(_.trim).filter(!_.isEmpty))
      .map(row => KickstarterProject.fromList(row))
      .filter(_.isDefined).map(_.get)
      .window(Seconds(windowLength), Seconds(slidingInterval))

    // Для каждого RDD считаем метрики
    projectsStream.foreachRDD(rdd => {
      DataConverter.saveDataToGS(rdd) // Сохраняем в хранилице

      // Текущее время
      val formatter = DateTimeFormatter.RFC_1123_DATE_TIME
      val time = ZonedDateTime.now(ZoneOffset.ofHours(4)).format(formatter)
      
      // Успешные проекты
      val successfulProjects = rdd.filter(p => p != null)
        .filter(project => Successful.equals(project.state))
        .map(project => project.country)
        .map((_, 1))
        .reduceByKey(_ + _)
        .map(r => Popularity(r._1, r._2, time))
        .sortBy(r => (-r.count, r.country), ascending = true)

      // Кол-во успешных проектов
      val countSuc = successfulProjects.map(_.count).sum().toInt

      // Неуспешные проекты
      val failedProjects = rdd.filter(p => p != null)
        .filter(project => Failed.equals(project.state))
        .map(project => project.country)
        .map((_, 1))
        .reduceByKey(_ + _)
        .map(r => Popularity(r._1, r._2, time))
        .sortBy(r => (-r.count, r.country), ascending = true)

      // Кол-во неуспешных проектов
      val countFailed = failedProjects.map(_.count).sum().toInt

      // Сохраняем в BigQuery топ-10 и общее кол-во
      DataConverter.saveData(successfulProjects.take(n), countSuc,
        failedProjects.take(n), countFailed, time, rdd.name, windowLength, projectId)

    })
  }
}
