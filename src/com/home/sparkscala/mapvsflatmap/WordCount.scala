package com.home.sparkscala.mapvsflatmap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.collection.Map

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount_SC")
    val lines = sc.textFile("downloads/SparkScala/book.txt")
    val words = lines.flatMap(line => line.split(" "))
    val wordCount: Map[String, Long] = words.countByValue()
    wordCount.foreach(println)

    // ToDO: remove punctuation, sort by highest word occurance
  }
}
