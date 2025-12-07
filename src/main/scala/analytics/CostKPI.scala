package analytics

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.SparkBuilder

object CostKPI {

  def main(args: Array[String]): Unit = {

    val spark = SparkBuilder.get("CostKPI")
    import spark.implicits._

    val curatedClaims = args(0)  
    val outputPath    = args(1) 

    val claims = spark.read.parquet(curatedClaims)

    // Provider KPIs
    val providerKPIs = claims
      .groupBy("provider_id")
      .agg(
        count("*").as("claim_count"),
        sum("cost").as("total_cost"),
        avg("cost").as("avg_cost"),
        max("cost").as("max_cost")
      )
      .orderBy(desc("total_cost"))


    val threshold = 1000.0
    val flagged = claims
      .withColumn("is_high_cost", col("cost") > threshold)


    providerKPIs.write.mode("overwrite").parquet(outputPath + "//provider_kpis")
    flagged.write.mode("overwrite").parquet(outputPath + "//flagged_claims")
    flagged.take(10).foreach(println)

    //println(s"Cost KPI: $outputPath")
    spark.stop()
  }
}
