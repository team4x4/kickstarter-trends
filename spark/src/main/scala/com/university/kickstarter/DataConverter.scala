package com.university.kickstarter

import java.io.{File, PrintWriter}

import com.university.kickstarter.KickstarterProjectsStreaming.Popularity
import org.apache.spark.rdd.RDD

import scala.io.Source

object DataConverter {

  // TODO save data to database
  def saveData(data: Array[Popularity], windowLength: Int): Unit = {

    val filePath = "src\\main\\resources\\output.txt"
    val writer = new PrintWriter(new File(filePath))
    for (element <- data) {
      writer.write(s"${element.country} -- ${element.count}\n")
    }
    writer.close()

    Source.fromFile(filePath).foreach {
      print
    }
  }

  // TODO save data to store
  def saveData(data: RDD[String]): Unit = {
    val filePath = s"src\\main\\resources\\output-${data.name}.txt"
    data.saveAsTextFile(filePath)
  }
}
