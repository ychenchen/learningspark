package com.embracesource.learningspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//spark-submit --master local --class com.embracesource.learningspark.SparkSqlAndDataFrameGuide /home/jimmy/learningspark-0.0.1.jar
object SparkSqlAndDataFrameGuide {
  def main(args: Array[String]) {
    //based on http://spark.apache.org/docs/1.5.1/sql-programming-guide.html#parquet-files
    //Overview
    //Spark SQL is a Spark module for structured data processing. It provides a programming abstraction called DataFrames and can also act as distributed SQL query engine.
    //Spark SQL can also be used to read data from an existing Hive installation. For more on how to configure this feature, please refer to the Hive Tables section.

    //DataFrames
    //A DataFrame is a distributed collection of data organized into named columns.
    //It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.
    //DataFrames can be constructed from a wide array of sources such as: /structured data files, tables in Hive, external databases, or existing RDDs.

    //Starting Point: SQLContext
    //The entry point into all functionality in Spark SQL is the SQLContext class, or one of its descendants. To create a basic SQLContext, all you need is a SparkContext.
    val conf = new SparkConf().setAppName("Spark Sql and Data Frame Guide")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    //In addition to the basic SQLContext, you can also create a HiveContext,
    //which provides a superset of the functionality provided by the basic SQLContext.

    //Creating DataFrames
    //With a SQLContext, applications can create DataFrames from an existing RDD, from a Hive table, or from data sources.
    //As an example, the following creates a DataFrame based on the content of a JSON file:
    val df = sqlContext.read.json("file:///data/hadoop/spark-1.5.0-bin-hadoop2.6/examples/src/main/resources/people.json")
    // Displays the content of the DataFrame to stdout
    // Show the content of the DataFrame
    df.show()
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin

    //DataFrame Operations
    // Print the schema in a tree format
    df.printSchema()
    // Select only the "name" column
    df.select("name").show()
    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()

    // Select people older than 21
    df.filter(df("age") > 21).show()
    // Count people by age
    df.groupBy("age").count().show()
    
    //In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic,
    //common math operations and more. The complete list is available in http://spark.apache.org/docs/1.5.1/api/scala/index.html#org.apache.spark.sql.DataFrame.
  }
}
