package com.embracesource.learningspark.sparkstreaming

object SparkStreamingProgrammingGuide {
  //To run this on your local machine, you need to first run Netcat (a small utility found in most Unix-like systems) as a data server by using
  //`$ nc -lk 9999`
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0");
    //Overview
    //Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams.
    //Data can be ingested from many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis, or TCP sockets,
    //and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window.
    //Finally, processed data can be pushed out to filesystems, databases, and live dashboards.
    //In fact, you can apply Spark¡¯s machine learning and graph processing algorithms on data streams.

    //Spark Streaming receives live input data streams and divides the data into batches,
    //which are then processed by the Spark engine to generate the final stream of results in batches.

    //Spark Streaming provides a high-level abstraction called discretized stream or DStream, which represents a continuous stream of data.
    //DStreams can be created either from input data streams from sources such as Kafka, Flume, and Kinesis,
    //or by applying high-level operations on other DStreams. Internally, a DStream is represented as a sequence of RDDs.

    //A Quick Example
    //Before we go into the details of how to write your own Spark Streaming program,
    //let¡¯s take a quick look at what a simple Spark Streaming program looks like.
    //Let¡¯s say we want to count the number of words in text data received from a data server listening on a TCP socket.
    //All you need to do is as follows.

    //First, we import the names of the Spark Streaming classes and some implicit conversions from StreamingContext into our environment
    //in order to add useful methods to other classes we need (like DStream).
    //StreamingContext is the main entry point for all streaming functionality.
    //We create a local StreamingContext with two execution threads, and a batch interval of 1 second.

    import org.apache.spark._
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    //Using this context, we can create a DStream that represents streaming data from a TCP source,
    //specified as hostname (e.g. localhost) and port (e.g. 9999).
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    //This lines DStream represents the stream of data that will be received from the data server. Each record in this DStream is a line of text.
    //Next, we want to split the lines by space characters into words.
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    //flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records from each record in the source DStream.
    //In this case, each line will be split into multiple words and the stream of words is represented as the words DStream. Next, we want to count these words.
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    //Note that when these lines are executed, Spark Streaming only sets up the computation it will perform when it is started,
    //and no real processing has started yet. To start the processing after all the transformations have been setup, we finally call
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    
  }
}