package matrix
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import utils.SparkBuilder

object Multiply {

  def main ( args: Array[String] ) {
    // val conf = new SparkConf().setAppName("Multiply")
    // val sc = new SparkContext(conf)


    val spark = SparkBuilder.get("Multiply")
    val sc = spark.sparkContext



    val M = sc.textFile(args(0)).map { line =>
                                      val parts = line.split(",")
                                      val i = parts(0).toInt
                                      val j = parts(1).toInt
                                      val v = parts(2).toDouble
                                      (j, (i, v))
                                    }

    val N = sc.textFile(args(1)).map { line =>
                                        val parts = line.split(",")
                                        val j = parts(0).toInt
                                        val k = parts(1).toInt
                                        val v = parts(2).toDouble
                                        (j, (k, v))
                                      }

    /*
    M.map{...}
      .join(N.map{...})
      .map{...}         // multiply corresponding elements
      .reduceByKey(...) // aggregation

    */

    val result = M
      .join(N)                    
      .map  { case (_, ((i, m_val), (k, n_val))) =>
                    ((i, k), m_val * n_val)     
            }
      .reduceByKey(_ + _)         
      

    //scala.io.StdIn.readLine()

    result.saveAsTextFile(args(2))
    result.take(10).foreach(println)


    sc.stop()

  }

  
}

