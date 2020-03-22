package com.university.kickstarter

import com.university.kickstarter.KickstarterStreaming.{KickstarterProject, Popularity}
import org.apache.spark.rdd.RDD
import java.util.UUID


// Сохранение данных
object DataConverter {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession = SparkSession.builder()
    .appName("spark-bigquery-demo")
    .getOrCreate()
  val bucket = s"kickstarter411"
  spark.conf.set("temporaryGcsBucket", bucket)

  import spark.implicits._

  // Сохранение данных в базу
  def saveData(dataSuc: Array[Popularity], countSuc: Int,
               dataFailed: Array[Popularity], countFailed: Int, time: String,
               name: String, windowLength: Int, projectId : String): Unit = {

    // Создаем dataframe из неусппешных проектов и их общего кол-ва
    val df = spark.sqlContext.createDataFrame(dataFailed)
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countFailed, time))))

    // Создаем временную таблицу для хранения этих данных
    df.createOrReplaceTempView("tempTable2")

    // Аналогично для успешных проектов
    val df2 = spark.sqlContext.createDataFrame(dataSuc)
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countSuc, time))))

    df2.createOrReplaceTempView("tempTable")

    // Соединяем таблицы
    val sqlQuery = "SELECT COALESCE(t1.country, t2.country) as country, t1.count as success, t2.count as failed, " +
      "COALESCE(t1.time_stamp, t2.time_stamp) as time_stamp" +
      " FROM tempTable as t1 FULL OUTER JOIN tempTable2 as t2 " +
      "ON t1.country = t2.country"
    print(sqlQuery)
    val wordCountDF3 = spark.sql(sqlQuery)

    // Сохраняем метрики в BigQuery.
    wordCountDF3.write.format("com.google.cloud.spark.bigquery")
      .option("table", projectId + ":dataset_kicks.kicks_metrics")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save()
  }

  // Сохранение в хранилице в формате parquet
  def saveDataToGS(data: RDD[KickstarterProject]): Unit = {
    val outputPath = s"gs://kickstarter411/output_data//data-${UUID.randomUUID()}"
    data.toDF().write.parquet(outputPath)
  }
}
