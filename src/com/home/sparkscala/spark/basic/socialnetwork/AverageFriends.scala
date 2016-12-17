package com.home.sparkscala.spark.basic.socialnetwork

import org.apache.spark.SparkContext

/**
  * Created by joey on 12/17/16.
  */
// Compute the average number of friends by age in a social network
object AverageFriends {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "AverageFriendsSC")
    // load each line of the source data into an RRD
    val data = sc.textFile("downloads/SparkScala/fakefriends.csv")

    // applies parseLine to every input row and returns a key value RRD
    // RDD of form (age, numFriends)
    val rrd = data.map(parseLine)

/*    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    val totalsByAge = rrd.mapValues(x => (x, 1))
      // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
      // adding together all the numFriends values and 1's respectively.
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect action. Kicks off computing the DAG
    val results = averagesByAge.collect()

    // print each row*/
    /*results.sorted.foreach(println)*/

  }



  def parseLine(line: String) = {
    val columns = line.split(",")
    val age = columns(2).toInt
    val numberFriends = columns(3).toInt
    // return a tuple key value pair
    (age, numberFriends)
  }



}
