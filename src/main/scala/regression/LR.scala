package regression
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import breeze.linalg._
//import shaded.breeze.linalg._

import utils.SparkBuilder

object LR {

  def main(args: Array[String]): Unit = {

    // val spark = SparkSession
    //   .builder
    //   .appName("Linear Regression Closed Form")
    //   .getOrCreate()

    val spark = SparkBuilder.get("Linear Regression (Closed Form)")
    val sc = spark.sparkContext

    import spark.implicits._

    val n = args(0).toInt
    val Xfile = args(1)
    val yfile = args(2)
    val output = args(3)

    // 1. Create a DataFrame X with columns (i: Int, j: Int, v: Double)
    //    and a DataFrame y with columns (i: Int, v: Double)

    val Xschema = "i Int, j Int, v Double"
    val yschema = "i Int, v Double"

    val Xdf = spark.read.format("csv").schema(Xschema).load(Xfile)
    Xdf.createOrReplaceTempView("X")

    val ydf = spark.read.format("csv").schema(yschema).load(yfile)
    ydf.createOrReplaceTempView("y")

    // 2. Compute X^T X

    val XtXdf = spark.sql(
      """
        SELECT X1.j AS j,
               X2.j AS k,
               SUM(X1.v * X2.v) AS value
        FROM X X1
        JOIN X X2
          ON X1.i = X2.i
        GROUP BY X1.j, X2.j
        ORDER BY j, k
      """
    )

    val XtXlocal = XtXdf.collect()

    // 3. Convert the result matrix to a Breeze Dense Matrix
    //    and compute the pseudo-inverse

    val XtX = DenseMatrix.zeros[Double](n, n)
    XtXlocal.foreach { row =>
      val j = row.getInt(0)
      val k = row.getInt(1)
      XtX(j, k) = row.getDouble(2)
    }

    // Breeze pseudo-inverse
    val XtXinv = pinv(XtX)   

    //scala.io.StdIn.readLine()

    // 4. Compute X^T y and convert it to a Breeze Vector
    val XtYdf = spark.sql(
      """
        SELECT X.j AS j,
               SUM(X.v * y.v) AS value
        FROM X
        JOIN y
          ON X.i = y.i
        GROUP BY X.j
        ORDER BY j
      """
    )

    //scala.io.StdIn.readLine()
    val XtYlocal = XtYdf.collect()

    val XtY = DenseVector.zeros[Double](n)
    XtYlocal.foreach { row =>
      val j = row.getInt(0)
      XtY(j) = row.getDouble(1)
    }

    // 5. Multiply (X^T X)^(-1) with X^T y to compute parameter vector θ
    val theta = XtXinv * XtY

    val thetaDF = theta.toArray.zipWithIndex
      .map { case (v, idx) => (idx, v) }
      .toSeq
      .toDF("j", "theta")


    thetaDF.show()
    thetaDF.write.mode("overwrite").csv(output)

    spark.stop()
  }
}
