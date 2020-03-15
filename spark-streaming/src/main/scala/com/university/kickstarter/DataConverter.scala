package com.university.kickstarter

import com.university.kickstarter.KickstarterStreaming.Popularity
import org.apache.spark.rdd.RDD

object DataConverter {
  import org.apache.spark.sql.SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("spark-bigquery-demo")
    .getOrCreate()
  val bucket = s"kickstarter411"
  spark.conf.set("temporaryGcsBucket", bucket)


  // TODO save data to database
  def saveData(dataSuc: Array[Popularity], countSuc: Int,
               dataFailed: Array[Popularity], countFailed: Int, time: String,
               name: String, windowLength: Int): Unit = {

    // Load data in from BigQuery.
    //    val wordsDF = spark.read.format("bigquery")
    //      .option("table","my-spark-project-270614:kikstarter_projects.projects")
    //      .load()
    //      .cache().schema
    //val df = wordsDF.sqlContext.createDataFrame(data).union(wordsDF)

    print(time)
    val df = spark.sqlContext.createDataFrame(dataFailed)
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countFailed, time))))

    df.createOrReplaceTempView("tempTable2")
    val df2 = spark.sqlContext.createDataFrame(dataSuc)
      .union(spark.sqlContext.createDataFrame(List(Popularity("total", countSuc, time))))

    df2.createOrReplaceTempView("tempTable")

    val wordCountDF = spark.sql(
      "SELECT country, SUM(count) AS success, time_stamp " +
        "FROM tempTable group by country, time_stamp")
      .createOrReplaceTempView("tmp")

    val wordCountDF2 = spark.sql(
      "SELECT country, SUM(count) AS failed, time_stamp " +
        "FROM tempTable2 group by country, time_stamp")
      .createOrReplaceTempView("tmp1")
    val sqlQuery = "SELECT COALESCE(t1.country, t2.country) as country, success, failed, " +
      "COALESCE(t1.time_stamp, t2.time_stamp) as time_stamp" +
      " FROM tmp as t1 FULL OUTER JOIN tmp1 as t2 " +
      "ON t1.country = t2.country"
    print(sqlQuery)
    val wordCountDF3 = spark.sql(sqlQuery)

    // Saving the data to BigQuery.
    wordCountDF3.write.format("bigquery")
      .option("table","my-spark-project-270614:q1.q3")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save()
  }

  // TODO save data to store
  def saveData(data: RDD[String]): Unit = {

    val outputPath = s"gs://kickstarter411/output_data"
    val filePath = s"src\\main\\resources\\output-${data.name}.txt"
    data.saveAsTextFile(outputPath)
  }
}
