package com.home.sparkscala.weather

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.max

/**
  * Minimum temperature for a year
  *
  * Data set is weather records from Paris
  *
  * data format csv:
  * weatherstation, date, weather type, temp
  */

object WeatherDataHighestPercipitation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Min_Weather_SC")
    val lines = sc.textFile("downloads/SparkScala/1800.csv")
    val parsedLines = lines.map(parseLines)
    // filter by a data type
    val minTemps = parsedLines.filter(x => x._2 == "PRCP")
    // transform to (stationId, prcp)
    val stationTempsRdd = minTemps.map(x => (x._1, x._3))
    // reduce by stationId, x,y are two diff temps, and we keep the minimum
    val minTempsByStation = stationTempsRdd.reduceByKey((x,y) => max(x,y))
    // collect, format and print the results
    val results = minTempsByStation.collect();
    results.sorted.foreach(r => {
      val station = r._1
      val temp = r._2
      println(s"$station max prcp: $temp")
    })


  }

  def parseLines(line:String) = {
    val columns = line.split(",")
    val weatherStationId = columns(0)
    val weatherType = columns(2)
    val temp = columns(3).toFloat * 0.1f //* (9.0f /  5.0f) + 32.0f
    // return the tuple
    (weatherStationId, weatherType, temp)
  }
}
