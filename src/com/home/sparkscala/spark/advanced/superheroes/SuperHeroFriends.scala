package com.home.sparkscala.spark.advanced.superheroes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * data file Marvel-graph.txt is of the format:
  * first number is the superhero, all subsequent numbers are "friends" that
  * appeared in the same comic as the superhero
  *
  * Marvel-names.txt is a mapping of superhero id to names
  *
  * 1. get the most popular superhero
  *
  *
  */
object SuperHeroFriends {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Popular_Superhero")
    val rows = sc.textFile("Marvel-graph.txt")
  }
}
