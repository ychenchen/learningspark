package com.embracesource.learningspark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.immutable.Map

object MLlibBasicStatistics {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0");
    val conf = new SparkConf().setMaster("local").setAppName("MLlibBasicStatistics")
    val sc = new SparkContext(conf)
    //Summary statistics
    //We provide column summary statistics for RDD[Vector] through the function colStats available in Statistics.
    //colStats() returns an instance of MultivariateStatisticalSummary,
    //which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the total count.
		import org.apache.spark.rdd.RDD
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
    
    val observations: RDD[Vector] = sc.parallelize(List(Vectors.dense(1, 2), Vectors.dense(3, 4), Vectors.dense(5, 6))) // an RDD of Vectors
    
    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean) // a dense vector containing the mean value for each column
    println(summary.variance) // column-wise variance
    println(summary.numNonzeros) // number of nonzeros in each column
    
    //Correlations
    //Calculating the correlation between two series of data is a common operation in Statistics.
    //In MLlib we provide the flexibility to calculate pairwise correlations among many series.
    //The supported correlation methods are currently Pearson¡¯s and Spearman¡¯s correlation.
    import org.apache.spark.SparkContext
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics
    
    val seriesX: RDD[Double] = sc.parallelize(List(1, 2, 3).map(_.toDouble)) // a series
    val seriesY: RDD[Double] = sc.parallelize(List(2, 3, 5).map(_.toDouble)) // must have the same number of partitions and cardinality as seriesX
    
    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
    // method is not specified, Pearson's method will be used by default. 
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println("correlation")
    println(correlation)
    
    val data: RDD[Vector] = sc.parallelize(List(Vectors.dense(1, 2), Vectors.dense(3, 4), Vectors.dense(5, 6))) // note that each Vector is a row and not a column
    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default. 
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(correlMatrix)
    
    //Stratified sampling
    import org.apache.spark.rdd.PairRDDFunctions
    
    val data1 = sc.parallelize(Seq(1 -> 1, 2 -> 2)) // an RDD[(K, V)] of any key value pairs
    val sampleMap = List((7, 0.4), (6, 0.8)).toMap
    val fractions: Map[Int, Double] = List((1, 0.4), (2, 0.8)).toMap // specify the exact fraction desired from each key
    
    // Get an exact sample from each stratum
    //val approxSample = data.sampleByKey(withReplacement = false, fractions)
    val exactSample = data1.sampleByKeyExact(withReplacement = false, fractions, 1)    
    println(exactSample)
    
    
    
  }
}