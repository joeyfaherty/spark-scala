package com.home.sparkscala

import org.apache.spark.SparkContext

/**
  * Minimum temperature for a year
  *
  * data format csv:
  * weatherstation, date, weather type, temp
  */

object WeatherData {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "Min_Weather_SC")
    val lines = sc.textFile("downloads/SparkScala/1800.csv")
    val parsedLines = lines.map(parseLines)
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    minTemps.map((x,y,z) => x, z)

  }

  def parseLines(line:String) = {
    val columns = line.split(",")
    val weatherStationId = columns(0)
    val weatherType = columns(2)
    val temp = columns(3).toFloat
    // return the tuple
    (weatherStationId, weatherType, temp)
  }
}
