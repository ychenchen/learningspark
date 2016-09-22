package com.embracesource.learningspark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MachineLeariningLibraryGuide {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0-cdh5.5.0");
    val conf = new SparkConf().setMaster("local").setAppName("Machine Learning Library Guide")
    val sc = new SparkContext(conf)
    //MLlib is Spark¡¯s machine learning (ML) library. Its goal is to make practical machine learning scalable and easy.
    //It consists of common learning algorithms and utilities, including classification, regression, clustering,
    //collaborative filtering, dimensionality reduction, as well as lower-level optimization primitives and higher-level pipeline APIs.
    //It divides into two packages:
    //spark.mllib contains the original API built on top of RDDs.
    //spark.ml provides higher-level API built on top of DataFrames for constructing ML pipelines.
    
    //MLlib-Data types
    //MLlib supports local vectors and matrices stored on a single machine, as well as distributed matrices backed by one or more RDDs.
    //Local vectors and local matrices are simple data models that serve as public interfaces.
    //The underlying linear algebra operations are provided by Breeze and jblas.
    //A training example used in supervised learning is called a ¡°labeled point¡± in MLlib.
    
    //Local Vector
    //A local vector has integer-typed and 0-based indices and double-typed values, stored on a single machine.
    //MLlib supports two types of local vectors: dense and sparse.
    //A dense vector is backed by a double array representing its entry values,
    //while a sparse vector is backed by two parallel arrays: indices and values.
    //For example, a vector (1.0, 0.0, 3.0) can be represented in dense format as [1.0, 0.0, 3.0]
    //or in sparse format as (3, [0, 2], [1.0, 3.0]), where 3 is the size of the vector.
    
    //The base class of local vectors is Vector, and we provide two implementations: DenseVector and SparseVector.
    //We recommend using the factory methods implemented in Vectors to create local vectors.
	  //spark.mllib: data types, algorithms, and utilities
	  import org.apache.spark.mllib.linalg.{Vector, Vectors}
	  // Create a dense vector (1.0, 0.0, 3.0).
	  val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
	  val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
	  // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
	  val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    //Note: Scala imports scala.collection.immutable.Vector by default, so you have to import org.apache.spark.mllib.linalg.Vector explicitly to use MLlib¡¯s Vector.
	  
	  //Labeled point
	  //A labeled point is a local vector, either dense or sparse, associated with a label/response.
	  //In MLlib, labeled points are used in supervised learning algorithms. We use a double to store a label,
	  //so we can use labeled points in both regression and classification.
	  //For binary classification, a label should be either 0 (negative) or 1 (positive).
	  //For multiclass classification, labels should be class indices starting from zero: 0, 1, 2, ....
	  
	  //A labeled point is represented by the case class LabeledPoint.
	  import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint
	  
	  // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    // Create a labeled point with a negative label and a sparse feature vector.
	  val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
	  //It is very common in practice to have sparse training data. MLlib supports reading training examples stored in LIBSVM format, which is the default format used by LIBSVM and LIBLINEAR. It is a text format in which each line represents a labeled sparse feature vector using the following format:
    //label index1:value1 index2:value2 ...
    //where the indices are one-based and in ascending order. After loading, the feature indices are converted to zero-based.
	  import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.rdd.RDD
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "D:\\Hadoop\\spark-1.5.0-bin-hadoop2.6\\data\\mllib\\sample_libsvm_data.txt")
    
    //Local Matrix
    //A local matrix has integer-typed row and column indices and double-typed values, stored on a single machine.
    //MLlib supports dense matrices, whose entry values are stored in a single double array in column-major order,
    //and sparse matrices, whose non-zero entry values are stored in the Compressed Sparse Column (CSC) format in column-major order.
    //For example, the following dense matrix
    //(1.0 2.0)
    //(3.0 4.0)
    //(5.0 6.0)
    //is stored in a one-dimensional array [1.0, 3.0, 5.0, 2.0, 4.0, 6.0] with the matrix size (3, 2).
	  import org.apache.spark.mllib.linalg.{Matrix, Matrices}
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    
    //Distributed matrix
    //A distributed matrix has long-typed row and column indices and double-typed values, stored distributively in one or more RDDs.
    //It is very important to choose the right format to store large and distributed matrices.
    //Converting a distributed matrix to a different format may require a global shuffle, which is quite expensive.
    //Three types of distributed matrices have been implemented so far.
    
    //The basic type is called RowMatrix. A RowMatrix is a row-oriented distributed matrix without meaningful row indices, e.g., a collection of feature vectors. 
    //RowMatrix
    //A RowMatrix is a row-oriented distributed matrix without meaningful row indices, backed by an RDD of its rows,
    //where each row is a local vector. Since each row is represented by a local vector,
    //the number of columns is limited by the integer range but it should be much smaller in practice.
    
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    //val rows1: RDD[Vector] = sc.textFile("D:\\numbers.txt").map(_.split(' ').map(_.toDouble)).map(line => Vectors.dense(line))  // an RDD of local vectors
    val rows1: RDD[Vector] = sc.parallelize(List(Vectors.dense(1, 2), Vectors.dense(3, 4), Vectors.dense(5, 6)))
    // Create a RowMatrix from an RDD[Vector].
    val mat1: RowMatrix = new RowMatrix(rows1)
    // Get its size.
    val m1 = mat1.numRows()  //3
    val n1 = mat1.numCols()  //2
    // QR decomposition 
    val qrResult = mat1.tallSkinnyQR(true)
        
    //IndexedRowMatrix
    //An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices.
    //It is backed by an RDD of indexed rows, so that each row is represented by its index (long-typed) and a local vector.
    import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
    val rows2: RDD[IndexedRow] = sc.parallelize(List(IndexedRow(0L, Vectors.dense(150,60,25)), IndexedRow(1L, Vectors.dense(300,80,40)))) // an RDD of indexed rows
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val mat2: IndexedRowMatrix = new IndexedRowMatrix(rows2)
    // Get its size.
    val m2 = mat2.numRows()  //2
    val n2 = mat2.numCols()  //3
    // Drop its row indices.
    val rowMat2: RowMatrix = mat2.toRowMatrix()

    //CoordinateMatrix
    //A CoordinateMatrix is a distributed matrix backed by an RDD of its entries.
    //Each entry is a tuple of (i: Long, j: Long, value: Double), where i is the row index, j is the column index,
    //and value is the entry value. A CoordinateMatrix should be used only when both dimensions of the matrix are huge and the matrix is very sparse.
    //A CoordinateMatrix can be created from an RDD[MatrixEntry] instance, where MatrixEntry is a wrapper over (Long, Long, Double).
    //A CoordinateMatrix can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix.
    //Other computations for CoordinateMatrix are not currently supported.
    
    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

    val entries1: RDD[MatrixEntry] = sc.parallelize(List(MatrixEntry(0,0,150), MatrixEntry(1,0,60), MatrixEntry(2,0,25), MatrixEntry(0,1,300),
                                                        MatrixEntry(1,1,80), MatrixEntry(2,1,40))) // an RDD of matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val mat3: CoordinateMatrix = new CoordinateMatrix(entries1)
    // Get its size.
    val m3 = mat3.numRows()
    val n3 = mat3.numCols()
    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    val indexedRowMatrix = mat3.toIndexedRowMatrix()
    
    //BlockMatrix
    //A BlockMatrix is a distributed matrix backed by an RDD of MatrixBlocks, where a MatrixBlock is a tuple of ((Int, Int), Matrix),
    //where the (Int, Int) is the index of the block, and Matrix is the sub-matrix at the given index with size rowsPerBlock x colsPerBlock.
    //BlockMatrix supports methods such as add and multiply with another BlockMatrix.
    //BlockMatrix also has a helper function validate which can be used to check whether the BlockMatrix is set up properly.
    //A BlockMatrix can be most easily created from an IndexedRowMatrix or CoordinateMatrix by calling toBlockMatrix.
    //toBlockMatrix creates blocks of size 1024 x 1024 by default.
    //Users may change the block size by supplying the values through toBlockMatrix(rowsPerBlock, colsPerBlock).
    import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}

    val entries2: RDD[MatrixEntry] = sc.parallelize(List(MatrixEntry(0,0,150), MatrixEntry(1,0,60), MatrixEntry(2,0,25), MatrixEntry(0,1,300),
                                                        MatrixEntry(1,1,80), MatrixEntry(2,1,40))) // an RDD of (i, j, v) matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries2)
    // Transform the CoordinateMatrix to a BlockMatrix
    val matA: BlockMatrix = coordMat.toBlockMatrix(2, 2).cache()
    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
    // Nothing happens if it is valid.
    matA.validate()
    // Calculate A^T A.
    val ata = matA.transpose.multiply(matA)
  }
}