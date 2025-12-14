%python
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#df = spark.read.parquet("/mnt/sparkdata/output/claims/")
#df = spark.read.parquet("output/claims")


claims_path      = "/mnt/sparkdata/output/claims"
provider_kpis    = "/mnt/sparkdata/output/provider_kpis/provider_kpis"
flagged_path     = "/mnt/sparkdata/output/provider_kpis/flagged_claims"


df = spark.read.parquet(claims_path)
provider_df = spark.read.parquet(provider_kpis)
flagged_df = spark.read.parquet(flagged_path)

pdf = df.toPandas()
provider_pdf = provider_df.toPandas()
flagged_pdf = flagged_df.toPandas()


print("Summary statistics:")
print(pdf['cost'].describe())



# Provider-level aggregation
provider_summary = (
    pdf.groupby("provider_id")['cost']
       .agg(['count', 'sum', 'mean', 'max'])
       .sort_values(by='sum', ascending=False)
)

print(provider_summary.head())

# Cost Dist plot
plt.figure(figsize=(10,5))
plt.hist(pdf['cost'], bins=30)
plt.title("Cost Distribution")
plt.xlabel("Cost")
plt.ylabel("Frequency")
plt.show()


#high cost 
high = flagged_pdf[flagged_pdf["is_high_cost"] == True]

plt.figure(figsize=(12,5))
plt.hist(high['cost'], bins=30, color='darkred', alpha=0.7)
plt.title("Distribution of High-Cost Claims")
plt.xlabel("Cost")
plt.ylabel("Frequency")
plt.show()

#risky provider
provider_risk = provider_pdf.copy()
provider_risk['risk_score'] = (
    provider_risk['avg_cost'] * 0.4 +
    provider_risk['total_cost'] * 0.4 +
    provider_risk['claim_count'] * 0.2
)

top_risk = provider_risk.sort_values("risk_score", ascending=False).head(15)

plt.figure(figsize=(12,6))
plt.bar(top_risk['provider_id'], top_risk['risk_score'], color='orange')
plt.xticks(rotation=45)
plt.title("Top 15 Providers by Composite Risk Score")
plt.xlabel("Provider ID")
plt.ylabel("Risk Score")
plt.show()

# demo show
top_patients = (
    pdf.groupby("patient_id")['cost']
       .sum()
       .sort_values(ascending=False)
       .head(10)
)

print(top_patients)
