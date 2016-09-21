package com.embracesource.learningspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//This object is used to test submit the file in windows platform.
//To do this, we should set the system property, this file refers to a hadoop installation dir.
//Notice that a winUtils.exe should be set under the bin directory.
//Note that applications should define a main() method instead of extending scala.App. Subclasses of scala.App may not work correctly.
object WindowsSimpleSparkApp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0");
    val serverFile = "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0\\etc\\hadoop\\core-site.xml"
    val conf = new SparkConf().setMaster("local").setAppName("Simple Spark Application")
    val sc = new SparkContext(conf)
    val serverData = sc.textFile(serverFile, 2).cache()
    val numAs = serverData.filter(line => line.contains("a")).count()
    val numBs = serverData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
