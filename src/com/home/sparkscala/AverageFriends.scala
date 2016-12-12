package com.home.sparkscala

import org.apache.spark.SparkContext

object AverageFriends {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "AverageFriendsSC")
    val data = sc.textFile("src/resources/averageFriends.txt")
    val rrd = data.map(parseLine)

    val totalsByAge = rrd.mapValues(x => (x, 1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    averagesByAge.sortBy(_._1).foreach(println)
  }



  def parseLine(line: String) = {
    val columns = line.split(",")
    val age = columns(2).toInt
    val numberFriends = columns(3).toInt
    (age, numberFriends)
  }



}
