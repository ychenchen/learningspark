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
    
    //Basic Concepts
    //Initializing StreamingContext
    //To initialize a Spark Streaming program, a StreamingContext object has to be created which is the main entry point of all Spark Streaming functionality.
    //A StreamingContext object can be created from a SparkConf object.

    //import org.apache.spark._
    //import org.apache.spark.streaming._
    //val conf = new SparkConf().setAppName(appName).setMaster(master)
    //val ssc = new StreamingContext(conf, Seconds(1))
    //However, for local testing and unit tests, you can pass ¡°local[*]¡± to run Spark Streaming in-process (detects the number of cores in the local system).
    //Note that this internally creates a SparkContext (starting point of all Spark functionality) which can be accessed as ssc.sparkContext.
    
    //A StreamingContext object can also be created from an existing SparkContext object.
    //import org.apache.spark.streaming._
    //val sc = ...                // existing SparkContext
    //val ssc = new StreamingContext(sc, Seconds(1))
    
    //After a context is defined, you have to do the following.
    //Define the input sources by creating input DStreams.
    //Define the streaming computations by applying transformation and output operations to DStreams.
    //Start receiving data and processing it using streamingContext.start().
    //Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
    //The processing can be manually stopped using streamingContext.stop().
    
    //Points to remember:
    //Once a context has been started, no new streaming computations can be set up or added to it.
    //Once a context has been stopped, it cannot be restarted.
    //Only one StreamingContext can be active in a JVM at the same time.
    //stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
    //A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext is stopped (without stopping the SparkContext)
    //before the next StreamingContext is created.

    //Discretized Streams (DStreams)
    //Discretized Stream or DStream is the basic abstraction provided by Spark Streaming.
    //It represents a continuous stream of data, either the input data stream received from source, or the processed data stream generated by transforming the input stream.
    //Internally, a DStream is represented by a continuous series of RDDs, which is Spark¡¯s abstraction of an immutable, distributed dataset 
    
    //Any operation applied on a DStream translates to operations on the underlying RDDs. 
    
    //Input DStreams and Receivers
    //Input DStreams are DStreams representing the stream of input data received from streaming sources.
    //Every input DStream (except file stream, discussed later in this section) is associated with a Receiver object
    //which receives the data from a source and stores it in Spark¡¯s memory for processing.
    
    //Spark Streaming provides two categories of built-in streaming sources.
    //Basic sources: Sources directly available in the StreamingContext API. Examples: file systems, socket connections, and Akka actors.
    //Advanced sources: Sources like Kafka, Flume, Kinesis, Twitter, etc. are available through extra utility classes.
    
    //Note that, if you want to receive multiple streams of data in parallel in your streaming application, you can create multiple input DStreams
    //This will create multiple receivers which will simultaneously receive multiple data streams.
    //But note that a Spark worker/executor is a long-running task, hence it occupies one of the cores allocated to the Spark Streaming application.
    //Therefore, it is important to remember that a Spark Streaming application needs to be allocated enough cores (or threads, if running locally) to process the received data,
    //as well as to run the receiver(s).
    
    //Basic Sources
    //Besides sockets, the StreamingContext API provides methods for creating DStreams from files and Akka actors as input sources.
    //File Streams: For reading data from files on any file system compatible with the HDFS API (that is, HDFS, S3, NFS, etc.), a DStream can be created as:
    //streamingContext.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)
    
    //Spark Streaming will monitor the directory dataDirectory and process any files created in that directory (files written in nested directories not supported).
    //Note that The files must have the same data format.
    //The files must be created in the dataDirectory by atomically moving or renaming them into the data directory.
    //Once moved, the files must not be changed. So if the files are being continuously appended, the new data will not be read.
    //For simple text files, there is an easier method streamingContext.textFileStream(dataDirectory). And file streams do not require running a receiver, hence does not require allocating cores.
    
    
    
    
    
    
    
    
  }
}