package com.university.kickstarter

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime, ZoneOffset}

import com.university.kickstarter.DataConverter.spark
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.util.Try


// Подсчет статистик из потока
object KickstarterStreaming {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  // Для метрик
  case class Popularity(country: String, count: Long)

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
      val df = rdd.toDF()

      // Текущее время
      val formatter = DateTimeFormatter.RFC_1123_DATE_TIME
      val timeZ = ZonedDateTime.now(ZoneOffset.ofHours(4))
      val time = timeZ.format(formatter)

      // Сохраняем в хранилице
      DataConverter.saveDataToGS(df, timeZ) // Сохраняем в хранилице

      // Успешные проекты
      val successfulProjects = df.filter(p => p != null)
        .filter("state = 'successful'")
        .groupBy("country")
        .agg(count("*").as("count"))
        .orderBy(desc("count"))

      // Кол-во успешных проектов
      val countSuc = successfulProjects
        .agg(sum("count")).first.getLong(0)

      // Неуспешные проекты
      val failedProjects = df.filter(p => p != null)
        .filter("state = 'failed'")
        .groupBy("country")
        .agg(count("*").as("count"))
        .orderBy(desc("count"))

      // Кол-во неуспешных проектов
      val countFailed = failedProjects.agg(sum("count")).first.getLong(0)

      DataConverter.saveDataDB(successfulProjects, countSuc,
        failedProjects, countFailed, time, rdd.name, windowLength, projectId)
    })
  }
}
