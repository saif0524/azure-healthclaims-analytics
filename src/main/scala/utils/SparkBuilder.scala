package utils

import org.apache.spark.sql.SparkSession
object SparkBuilder {
  def get(app: String) = {
    SparkSession.builder()
      .appName(app)
//      .master("local[*]")
      .getOrCreate()
  }
}
