package com.home.sparkscala.spark.basic.weather

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

object WeatherData {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Min_Weather_SC")
    val lines = sc.textFile("downloads/SparkScala/1800.csv")
    val parsedLines = lines.map(parseLines)
    // filter by a data type
    val minTemps = parsedLines.filter(x => x._2 == "TMAX")
    // we no longer need the TMIN type as each row now has this
    // so we transform a 3 tuple rrd to a kv rrd
    // transform to (stationId, temp)
    val stationTempsRdd = minTemps.map(x => (x._1, x._3.toFloat))
    // reduce by stationId, x,y are two diff temps, and we keep the minimum
    val minTempsByStation = stationTempsRdd.reduceByKey((x,y) => max(x,y))
    // collect, format and print the results
    minTempsByStation.collect().sorted.foreach(r => {
      val station = r._1
      val temp = r._2
      val formattedTemp = f"$temp%.2f degrees C"
      println(s"$station max temp: $formattedTemp")
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
