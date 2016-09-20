package com.embracesource.learningspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//spark-submit --master local --class com.embracesource.learningspark.SparkProgrammingGuid /home/jimmy/learningspark-0.0.1.jar
object SparkProgrammingGuid {
  def main(args: Array[String]) {
    //based on http://spark.apache.org/docs/1.5.1/programming-guide.html#background
    //Initializing Spark
    //Only one SparkContext may be active per JVM. You must stop() the active SparkContext before creating a new one.
    val conf = new SparkConf().setAppName("ProgrammingGuide4Scala").setMaster("local")
    val sc = new SparkContext(conf)

    //Using shell
    //./bin/spark-shell --master local[3]

    //Resilient Distributed Datasets (RDDs)
    //Parallelized Collections
    //Parallelized collections are created by calling SparkContext parallelize method on an existing collection in your driver program (a Scala Seq).
    //The elements of the collection are copied to form a distributed dataset that can be operated on in parallel.
    //For example, here is how to create a parallelized collection holding the numbers 1 to 5:
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.reduce((a, b) => a + b)

    //External Datasets
    //Spark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase,
    //Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
    //Text file RDDs can be created using SparkContext textFile method.
    //This method takes an URI for the file
    val distData1 = "hdfs://hadoop01:9000/data/input/words.data"
    val distFile = sc.textFile(distData1, 2)  //the second parameter is optional
    distFile.map(s => s.length).reduce((a, b) => a + b)
    //Once created, distFile can be acted on by dataset operations.
    //The textFile method takes an optional second argument for controlling the number of partitions of the file.
    //By default, Spark creates one partition for each block of the file (blocks being 64MB by default in HDFS),
    //but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.    
    
    //RDD Operations
    //RDDs support two types of operations: transformations, which create a new dataset from an existing one, and actions,
    //which return a value to the driver program after running a computation on the dataset.
    //By default, each transformed RDD may be recomputed each time you run an action on it.
    //However, you may also persist an RDD in memory using the persist (or cache) method,
    //in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. 
    
    //Passing Functions to Spark
    //There are two recommended ways to do this:
    //A:Anonymous function syntax, which can be used for short pieces of code.
    //B:Static methods in a global singleton object.
    object MyFunctions {
      def func1(s: String): String = { s }
    }
    distFile.map(MyFunctions.func1)
    
    //Understanding closures
    //Accumulators in Spark are used specifically to provide a mechanism for safely updating a variable when execution is split up across worker nodes in a cluster. 
    
    //Printing elements of an RDD
    //To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node thus:
    //rdd.collect().foreach(println).
    //This can cause the driver to run out of memory, though, because collect() fetches the entire RDD to a single machine;
    //if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
    
    //Working with Key-Value Pairs
    //For example, the following code uses the reduceByKey operation on key-value pairs to count how many times each line of text occurs in a file:
    val lines = sc.textFile("hdfs://hadoop01:9000/data/input/words.data")
    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    
    //Shuffle operations
    //In Spark, data is generally not distributed across partitions to be in the necessary place for a specific operation.
    //During computations, a single task will operate on a single partition - thus, to organize all the data for a single reduceByKey reduce task to execute,
    //Spark needs to perform an all-to-all operation. It must read from all partitions to find all the values for all keys,
    //and then bring together values across partitions to compute the final result for each key - this is called the shuffle.
    
    //RDD Persistence
    //One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations.
    //When you persist an RDD, each node stores any partitions of it that it computes
    //in memory and reuses them in other actions on that dataset (or datasets derived from it). 
    //Removing Data
    //Spark automatically monitors cache usage on each node and drops out old data partitions in a least-recently-used (LRU) fashion.
    //If you would like to manually remove an RDD instead of waiting for it to fall out of the cache, use the RDD.unpersist() method.
    
    //Shared Variables
    //A:Broadcast Variables
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    broadcastVar.value  //Array[Int] = Array(1, 2, 3)
    //B:Accumulators    
    val accum = sc.accumulator(0, "My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
    println(accum.value)  //10
  }
}
