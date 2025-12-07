package etl

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.SparkBuilder

object ClaimsETL {

  def main(args: Array[String]): Unit = {

    val spark = SparkBuilder.get("ClaimsETL")
    import spark.implicits._

    val claimsPath = args(0)              // claims.csv
    val transactionsPath = args(1)        // claims_transactions.csv
    val outputPath = args(2)              // curated output folder

    // ------------------------------------------------------------------------------
    // 1. Load raw claims (header-level claims data)
    // ------------------------------------------------------------------------------
    val rawClaims = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(claimsPath)

    // Rename key columns
    val claims = rawClaims
      .withColumnRenamed("Id", "claim_id")
      .withColumnRenamed("PATIENTID", "patient_id")
      .withColumnRenamed("PROVIDERID", "provider_id")
      .withColumnRenamed("DIAGNOSIS1", "diagnosis_code")
      .withColumn("service_date", to_timestamp(col("SERVICEDATE")))

    // ------------------------------------------------------------------------------
    // 2. Load transactions to compute claim-level cost
    // ------------------------------------------------------------------------------
    val rawTx = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(transactionsPath)

    val tx = rawTx
      .withColumnRenamed("CLAIMID", "claim_id")
      .withColumn("amount", col("AMOUNT").cast("double"))

    // Sum of all transaction amounts per claim
    val claimCosts = tx
      .groupBy("claim_id")
      .agg(sum("amount").as("cost"))

    // ------------------------------------------------------------------------------
    // 3. Join header claims with cost summary
    // ------------------------------------------------------------------------------
    val enrichedClaims = claims
      .join(claimCosts, Seq("claim_id"), "left")
      .filter(col("cost").isNotNull)  // remove claims with no financial activity

    // ------------------------------------------------------------------------------
    // 4. Dedup + final clean
    // ------------------------------------------------------------------------------
    val finalClaims = enrichedClaims
      .dropDuplicates("claim_id")

    // ------------------------------------------------------------------------------
    // 5. Write curated output
    // ------------------------------------------------------------------------------
    finalClaims.write.mode("overwrite").parquet(outputPath)

    println(s"Claims ETL completed successfully at: $outputPath")

    spark.stop()
  }
}
