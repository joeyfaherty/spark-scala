package com.home.sparkscala.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * add up amount spent by the customer. sort results by highest spender
  *
  * csv is in the format customerId, itemId, amountSpent
  */

object HighestSpenders {

  def extractIdAndSpend(row: String) = {
    val columns = row.split(",")
        // return tuple
    (columns(0).toInt, columns(2).toFloat)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR);

    val sc = new SparkContext("local[*]", "High Spenders")

    val rows = sc.textFile("downloads/SparkScala/customer-orders.csv")

    val mapIdAmountSpent = rows.map(extractIdAndSpend)

    // sum the total spend of each customerId
    val totalSpentById = mapIdAmountSpent.reduceByKey((x, y) => x + y)

    // flip the key and value
    val flippedMap = totalSpentById.map(x => (x._2, x._1))

    flippedMap.sortByKey()

    // sort by the highest spenders
    flippedMap.collect().sortBy(x => x._1)(Ordering[Float].reverse).foreach(x => {
      val spend = x._1
      val id = x._2
      // note: need to use variable spend here as x._2 wont be recognized in $
      val formattedSpend = f"$spend%.2f"
      println(s"total spend of customer $id is: $formattedSpend")
    })

  }


}
