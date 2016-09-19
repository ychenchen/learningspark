package com.embracesource.learningspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//spark-submit --master local --class com.embracesource.learningspark.SimpleSparkApp /home/jimmy/learningspark-0.0.1.jar
//Note that applications should define a main() method instead of extending scala.App. Subclasses of scala.App may not work correctly.
object SimpleSparkApp {
  def main(args: Array[String]) {
    //read file from local file system
    //val serverFile = "file:///data/hadoop/spark-1.5.0-cdh5.5.0/conf/spark-env.sh"
    //read file from hdfs
    val serverFile = "hdfs://hadoop01:9000/data/input/words.data"
    val conf = new SparkConf().setAppName("Simple Spark Application")
    val sc = new SparkContext(conf)
    val serverData = sc.textFile(serverFile, 2).cache()
    val numAs = serverData.filter(line => line.contains("a")).count()
    val numBs = serverData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
