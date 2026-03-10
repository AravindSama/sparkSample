package scala.practice
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_Object {
   def main(arg:Array[String]):Unit=
  {
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val inputRDD = sc.textFile("file:///C:/Spark/test_log.log")
    //inputRDD.take(5).foreach(println)
    
    val warnRDD =inputRDD.filter(w=>w.contains("WARN"))
    //warnRDD.take(5).foreach(println)
          println("The warn count is "+warnRDD.count())

    val errorRDD =inputRDD.filter(w=>w.contains("ERROR"))
   // errorRDD.take(5).foreach(println)
        println("The error count is "+errorRDD.count())

    val unionRDD= warnRDD.union(errorRDD)
    println("The union count is "+unionRDD.count())

//    val data=sc.textFile("file:///home/cloudera/datasets/BigBasket.csv")
//    data.take(5).foreach(println)
//    sc.setLogLevel("Error")
  }
}