package covariance
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import utils.SparkBuilder

object Covariance {

  def main ( args: Array[String] ) {
    // val conf = new SparkConf()
    //   .setAppName("Covariance")

    // val sc = new SparkContext(conf)


    val spark = SparkBuilder.get("Covariance")
    val sc = spark.sparkContext

    val n = args(0).toInt

    val raw = sc.textFile(args(1))
                .map { line =>
                      val p = line.split(",")
                      (p(0).toInt, p(1).toInt, p(2).toDouble)
                     }
    /*  
      First, center the data by computing the average of each column and subtracting that average from each rating in the same column. 
      Next, implement Covariance matrix on Spark using the following formula: Cov =  Mult(X^T, X) / (n-1)
    */

    val colAvg = raw.map { case (u, i, r) => (i, (r, 1)) }
                    .reduceByKey  { case ((s1, c1), (s2, c2)) =>
                                              (s1 + s2, c1 + c2)
                                  }
                    .mapValues { case (sum, count) => sum / count }


    val centered = raw.map { case (u, i, r) => (i, (u, r)) }
                      .join(colAvg)
                      .map  { case (i, ((u, r), avg)) =>
                                          (u, i, r - avg)
                            }

    centered.collect().foreach(println)

    //Cov =  Mult(XT, X) / (n-1)
    val X = centered.map { case (u, i, cr) =>  (u, (i, cr))}
    X.collect().foreach(println)



    val prod = X.join(X)
                .map { case (u, ((i1, cr1), (i2, cr2))) =>
                                      ((i1, i2), cr1 * cr2)
                      }

    prod.collect().foreach(println)

    val cov = prod.reduceByKey(_ + _)
                  .map { case ((i1, i2), sum) =>
                                (i1, i2, sum / (n - 1))
                      }


    // cov.collect().foreach(println)
    //cov.map { case ((i,j),v) => s"$i,$j,$v" }
    //    .saveAsTextFile(args(2))
     cov.saveAsTextFile(args(2))

    // scala.io.StdIn.readLine()
    sc.stop()
    
  }

  
}