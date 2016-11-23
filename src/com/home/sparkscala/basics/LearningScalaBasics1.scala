package com.home.sparkscala.basics

/**
  * Created by joey on 11/22/16.
  */
object LearningScalaBasics1 {

  def main(args: Array[String]): Unit = {
/*    // values are immutable
    val hello: String = "Bonjour!"
    println(hello)

    // variables are mutable
    var helloThere: String = hello
    println(helloThere)*/

    var i = 0;
    for (x <- 0 to 9) {
      i match {
        case 0 => println(0)
        case 1 | 2 => println(1)
        case _ => println(x._i))
      }
      i += 1;
    }
  }


}
