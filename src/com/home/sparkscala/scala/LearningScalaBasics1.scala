package com.home.sparkscala.scala

/**
  * Created by joey on 11/22/16.
  */
object LearningScalaBasics1 {

  def main(args: Array[String]): Unit = {
    // values are immutable
    val hello: String = "Bonjour!"
    println(hello)

    // variables are mutable
    var helloThere: String = hello
    println(helloThere)
  }


}
