package com.university.kickstarter

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.university.kickstarter.KickstarterStreaming.{KickstarterProject, Popularity}

import org.apache.spark.sql.DataFrame


// Сохранение данных
object DataConverter {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession = SparkSession.builder()
    .appName("spark")
    .getOrCreate()
  val bucket = s"kickstarter411"
  spark.conf.set("temporaryGcsBucket", bucket)

  // Сохранение данных в базу
//  def saveData(dataSuc: Array[Popularity], countSuc: Int,
//               dataFailed: Array[Popularity], countFailed: Int, time: String,
//               name: String, windowLength: Int, projectId : String): Unit = {
//
//    // Создаем dataframe из неусппешных проектов и их общего кол-ва
//    val df = spark.sqlContext.createDataFrame(dataFailed)
//      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countFailed, time))))
//
//    // Создаем временную таблицу для хранения этих данных
//    df.createOrReplaceTempView("tempTable2")
//
//    // Аналогично для успешных проектов
//    val df2 = spark.sqlContext.createDataFrame(dataSuc)
//      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countSuc, time))))
//
//    df2.createOrReplaceTempView("tempTable")
//
//    // Соединяем таблицы
//    val sqlQuery = "SELECT COALESCE(t1.country, t2.country) as country, t1.count as success, t2.count as failed, " +
//      "COALESCE(t1.time_stamp, t2.time_stamp) as time_stamp" +
//      " FROM tempTable as t1 FULL OUTER JOIN tempTable2 as t2 " +
//      "ON t1.country = t2.country"
//    val wordCountDF3 = spark.sql(sqlQuery)
//
//    // Сохраняем метрики в BigQuery.
//    wordCountDF3.write.format("com.google.cloud.spark.bigquery")
//      .option("table", projectId + ":dataset_kicks.kicks_metrics")
//      .mode(org.apache.spark.sql.SaveMode.Overwrite)
//      .save()
//  }

  // Сохранение в хранилице в формате parquet
  def saveDataToGS(data: DataFrame, time: ZonedDateTime): Unit = {
    val formatter = DateTimeFormatter.ISO_LOCAL_TIME
    val outputPath = s"gs://kickstarter411/output_data/data/${time.toLocalDate}/${time.format(formatter)}"
    data.write.parquet(outputPath)
  }

  def readParquet(filePath: String): Unit = {
    val df = spark.sqlContext.read.parquet(filePath)
    df.show()
  }


  // Сохранение данных в базу (глобально)
  def saveDataDB(dataSuc: DataFrame, countSuc: Long,
               dataFailed: DataFrame, countFailed: Long, time: String,
               name: String, windowLength: Int, projectId : String): Unit = {

    // Load data in from BigQuery.
    val df_main = spark.read.format("com.google.cloud.spark.bigquery")
          .option("table", projectId + ":dataset_kicks.kicks_metrics1")
          .load()
          .cache()
    // Создаем dataframe из неусппешных проектов и их общего кол-ва
    val df = dataFailed
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countFailed))))

    // Создаем временную таблицу для хранения этих данных
    df.createOrReplaceTempView("tempTable2")

    // Аналогично для успешных проектов
    val df2 = dataSuc
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countSuc))))

    df2.createOrReplaceTempView("tempTable")

    // Соединяем таблицы
    val sqlQuery = "SELECT COALESCE(t1.country, t2.country) as country, t1.count as success, t2.count as failed" +
      " FROM tempTable as t1 FULL OUTER JOIN tempTable2 as t2 " +
      "ON t1.country = t2.country"
    val wordCountDF3 = spark.sql(sqlQuery).union(df_main)
    wordCountDF3.createOrReplaceTempView("main_table")

    // Соединяем таблицы
    val sqlQueryMain = "SELECT country, sum(success) as success, sum(failed) as failed" +
      " FROM main_table " +
      "GROUP BY country"
    val wordCountDFMain = spark.sql(sqlQueryMain)
    wordCountDFMain.createOrReplaceTempView("final_table")

    // Сохраняем данные в BigQuery.
    wordCountDFMain.write.format("com.google.cloud.spark.bigquery")
      .option("table", projectId + ":dataset_kicks.kicks_metrics1")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save()

    val sql = "SELECT country, success" +
      " FROM final_table ORDER BY success DESC LIMIT 11"
    val sql2 = "SELECT country, failed" +
      " FROM final_table ORDER BY failed DESC LIMIT 11"
    val metrics1 = spark.sql(sql)
    val metrics2 = spark.sql(sql2)
    metrics1.createOrReplaceTempView("met1")
    metrics2.createOrReplaceTempView("met2")

    // Соединяем таблицы
    val sqlQueryMetric = "SELECT COALESCE(t1.country, t2.country) as country, t1.success as success, " +
      "t2.failed as failed" +
      " FROM met1 as t1 FULL OUTER JOIN met2 as t2 " +
      "ON t1.country = t2.country"
    val wordCountDFFinal = spark.sql(sqlQueryMetric)

    wordCountDFFinal.write.format("com.google.cloud.spark.bigquery")
      .option("table", projectId + ":dataset_kicks.kicks_metricsF")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save()
  }
}
