import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#df = spark.read.parquet("/mnt/sparkdata/curated/claims/")
df = spark.read.parquet("output/claims")

pdf = df.toPandas()

print("Summary statistics:")
print(pdf['cost'].describe())


# Provider-level aggregation
provider_summary = (
    pdf.groupby("provider_id")['cost']
       .agg(['count', 'sum', 'mean', 'max'])
       .sort_values(by='sum', ascending=False)
)

print(provider_summary.head())

# plot - Cost Dist
plt.figure(figsize=(10,5))
plt.hist(pdf['cost'], bins=30)
plt.title("Cost Distribution")
plt.xlabel("Cost")
plt.ylabel("Frequency")
plt.show()

# demo show
top_patients = (
    pdf.groupby("patient_id")['cost']
       .sum()
       .sort_values(ascending=False)
       .head(10)
)

print(top_patients)
