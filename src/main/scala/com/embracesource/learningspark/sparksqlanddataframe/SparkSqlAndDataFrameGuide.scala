package com.embracesource.learningspark.sparksqlanddataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Person(name: String, age: Int)

//spark-submit --master local --class com.embracesource.learningspark.SparkSqlAndDataFrameGuide /home/jimmy/learningspark-0.0.1.jar
object SparkSqlAndDataFrameGuide {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0");
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
    val conf = new SparkConf().setMaster("local").setAppName("Spark Sql and Data Frame Guide")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    //In addition to the basic SQLContext, you can also create a HiveContext,
    //which provides a superset of the functionality provided by the basic SQLContext.

    //Creating DataFrames
    //With a SQLContext, applications can create DataFrames from an existing RDD, from a Hive table, or from data sources.
    //As an example, the following creates a DataFrame based on the content of a JSON file:
    //    //val df = sqlContext.read.json("file:///data/hadoop/spark-1.5.0-bin-hadoop2.6/examples/src/main/resources/people.json")
    val df = sqlContext.read.json("D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
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

    //Running SQL Queries Programmatically
    //The sql function on a SQLContext enables applications to run SQL queries programmatically and returns the result as a DataFrame.
    //val sqlContext = ...  // An existing SQLContext
    //val df = sqlContext.sql("SELECT * FROM table")

    //Interoperating with RDDs
    //Spark SQL supports two different methods for converting existing RDDs into DataFrames.
    //The first method uses reflection to infer the schema of an RDD that contains specific types of objects.
    //This reflection based approach leads to more concise code and works well when you already know the schema while writing your Spark application.
    //The second method for creating DataFrames is through a programmatic interface that allows you to construct a schema and then apply it to an existing RDD.
    //While this method is more verbose, it allows you to construct DataFrames when the columns and their types are not known until runtime.

    //Inferring the Schema Using Reflection
    //The Scala interface for Spark SQL supports automatically converting an RDD containing case classes to a DataFrame.
    //The case class defines the schema of the table. The names of the arguments to the case class are read using reflection and become the names of the columns.
    //Case classes can also be nested or contain complex types such as Sequences or Arrays.
    //This RDD can be implicitly converted to a DataFrame and then be registered as a table. Tables can be used in subsequent SQL statements.

    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface.
    //case class Person(name: String, age: Int)  //in this situation, case class should be defined outside the object class   

    // Create an RDD of Person objects and register it as a table.
    val people = sc.textFile("D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim().toInt)).toDF()
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name:" + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println) // Map("name" -> "Justin", "age" -> 19)

    //Programmatically Specifying the Schema
    //When case classes cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields
    //will be projected differently for different users), a DataFrame can be created programmatically with three steps.
    //Create an RDD of Rows from the original RDD;
    //Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    //Apply the schema to the RDD of Rows via createDataFrame method provided by SQLContext.

    // Create an RDD
    val people1 = sc.textFile("D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt")
    // The schema is encoded in a string
    val schemaString = "name age"
    // Import Row.
    import org.apache.spark.sql.Row;
    // Import Spark SQL data types
    import org.apache.spark.sql.types.{ StructType, StructField, StringType };
    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people1.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people1")
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    //Data Sources
    //Spark SQL supports operating on a variety of data sources through the DataFrame interface.
    //A DataFrame can be operated on as normal RDDs and can also be registered as a temporary table.
    //Registering a DataFrame as a table allows you to run SQL queries over its data.
    //This section describes the general methods for loading and saving data using the Spark Data Sources
    //and then goes into specific options that are available for the built-in data sources.

    //Generic Load/Save Functions
    //In the simplest form, the default data source (parquet unless otherwise configured by spark.sql.sources.default) will be used for all operations.
    val df1 = sqlContext.read.load("D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\users.parquet")
    //df1.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    //Manually Specifying Options
    //You can also manually specify the data source that will be used along with any extra options that you would like to pass to the data source.
    //Data sources are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet),
    //but for built-in sources you can also use their short names (json, parquet, jdbc). DataFrames of any type can be converted into other types using this syntax.

    val df2 = sqlContext.read.format("json").load("D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
    //df2.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    //Save Modes
    //omit this section

    //Saving to Persistent Tables
    //When working with a HiveContext, DataFrames can also be saved as persistent tables using the saveAsTable command.
    //Unlike the registerTempTable command, saveAsTable will materialize the contents of the dataframe and create a pointer to the data in the HiveMetastore.
    //Persistent tables will still exist even after your Spark program has restarted, as long as you maintain your connection to the same metastore.
    //A DataFrame for a persistent table can be created by calling the table method on a SQLContext with the name of the table.

    //By default saveAsTable will create a ¡°managed table¡±, meaning that the location of the data will be controlled by the metastore.
    //Managed tables will also have their data deleted automatically when a table is dropped.

    //Parquet Files
    //Parquet is a columnar format that is supported by many other data processing systems.
    //Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data.
    //Loading Data Programmatically
    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.

    // people: An RDD of case class objects, from the previous example.
    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
    //people.write.parquet("people.parquet")
    // Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a DataFrame.
    //val parquetFile = sqlContext.read.parquet("people.parquet")
    //Parquet files can also be registered as tables and then used in SQL statements.
    //parquetFile.registerTempTable("parquetFile")
    //val teenagers1 = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    //teenagers1.map(t => "Name: " + t(0)).collect().foreach(println)

    //Partition Discovery
    //omit this section

    //Schema Merging
    //Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we turned it off by default starting from 1.5.0. You may enable it by
    //setting data source option mergeSchema to true when reading Parquet files (as shown in the examples below), or
    //setting the global SQL option spark.sql.parquet.mergeSchema to true.

    //Hive metastore Parquet table conversion
    //When reading from and writing to Hive metastore Parquet tables, Spark SQL will try to use its own Parquet support instead of Hive SerDe for better performance. This behavior is controlled by the spark.sql.hive.convertMetastoreParquet configuration, and is turned on by default.
    //Hive/Parquet Schema Reconciliation
    //There are two key differences between Hive and Parquet from the perspective of table schema processing.
    //Hive is case insensitive, while Parquet is not
    //Hive considers all columns nullable, while nullability in Parquet is significant

    //JSON Datasets
    //Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
    //This conversion can be done using SQLContext.read.json() on either an RDD of String, or a JSON file.
    //Note that the file that is offered as a json file is not a typical JSON file. Each line must contain a separate,
    //self-contained valid JSON object. As a consequence, a regular multi-line JSON file will most often fail.
    val path = "D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json"
    val people2 = sqlContext.read.json(path)
    // The inferred schema can be visualized using the printSchema() method.
    people2.printSchema()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this DataFrame as a table.
    people2.registerTempTable("people2")
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers2 = sqlContext.sql("SELECT name FROM people2 WHERE age >= 13 AND age <= 19")
    teenagers2.map(t => "Name: " + t(0)).collect().foreach(println)
    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)

    //Hive Tables
    //When working with Hive one must construct a HiveContext, which inherits from SQLContext,
    //and adds support for finding tables in the MetaStore and writing queries using HiveQL.
    //Users who do not have an existing Hive deployment can still create a HiveContext. When not configured by the hive-site.xml,
    //the context automatically creates metastore_db and warehouse in the current directory.

    // sc is an existing SparkContext.
    val hiveSqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveSqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveSqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    // Queries are expressed in HiveQL
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)

  }
}
