package com.home.sparkscala.spark.advanced.movieratings

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * data is in the format userId, movieId, rating, timestamp
  * 1. find the most popular movie
  * 2. from u.item file map movie id's to names
  */
object RatingsCounter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FindMostPopularMovie")

    // broadcast map to cluster
    var movieNameDictionary = sc.broadcast(loadMovieNames())

    val rows = sc.textFile("data/ml-100k/u.data")

    val movieIds = rows.map(x => {
      val fields = x.split("\t")
      val movieId = fields(1).toInt
      (movieId)
    })

    val results = movieIds
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey()

    val namesMappedToIds = results.map(x => (movieNameDictionary.value(x._2), x._1))
      .collect()
      .foreach(println)
  }

  /**
    * broadcast varibale to be available to each node on the cluster
    * @return
    */
  def loadMovieNames() : Map[Int, String] = {
    // handle UTF-8 character encoding
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    lines.foreach(x => {
      var fields = x.split('|')
      if (fields.length > 1) {
        // map id to the name with ->
        movieNames += (fields(0).toInt -> fields(1))
      }
    })
    return movieNames
  }
}
