package com.home.sparkscala.spark.basic.movieratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    // [*] distributed throughout all available cores
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    // lines contains rows of the ratings
    // eg 196 242 3 8884832729903
    // (The file format is userID, movieID, rating, timestamp)
    val lines = sc.textFile("data/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs "\t", and extract the third field. (2)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    // countByValue is an action which causes Spark to compute the RRD
    // returns a map of results in the form (ratingNumber, ratingCount)
    /**
      * under the hood: when we call the countByValue action it creates a execution plan of stages
      * stage 1) map.. can be processed individually and in parallel
      * stage 2) shuffle ..
      * stages can then be split into tasks that can be distributed to run in parallel and on different nodes
      * countByValue() causes a shuffle operation (similar to reduce phase in MR) [where map results running across the cluster need to be aggregated]
      * map() has a 1 to 1 relationship between in input and output of each row of the RRD
      */
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    // covert to a seq so we can sort by rating
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
