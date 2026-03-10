package scala.practice
import scala.io.Source

object Basics {
   def main(arg:Array[String]):Unit=
  {
    val simple_read=Source.fromFile("c:/Spark/America.txt")
    val input= simple_read.mkString
    println(input)
    simple_read.close()
  }
}