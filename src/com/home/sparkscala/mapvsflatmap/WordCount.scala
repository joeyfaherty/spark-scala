package com.home.sparkscala.mapvsflatmap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.collection.Map

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // in this example we will see strange results if we use all local cores
    val sc = new SparkContext("local", "WordCount_SC")
    val lines = sc.textFile("downloads/SparkScala/book.txt")
    //val words = lines.flatMap(line => line.split(" "))
    // split the string up in words, and + means can be 1 or more words
    val words = lines.flatMap(line => line.split("\\W+"))
    // normalize all words to lowercase
    // 1 to 1 relationship between input and output so we use map() here
    val lowercaseWords = words.map(x => x.toLowerCase())

    // filter out words that don't add value to analysis
    val filtered = lowercaseWords.filter(w =>
      !w.equals("the")
        && !w.equals("a")
        && !w.equals("to")
        && !w.equals("of")
        && !w.equals("it")
        && !w.equals("that")
        && !w.equals("and")
        && !w.equals("is")
    )

    val wordCount = filtered.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)

    // flip the key values around and sort by key
    val sortedWords = wordCount.map(x => (x._2, x._1)).sortByKey()

    // now flip it back and print it out formatted
    for (result <- sortedWords) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }
}
