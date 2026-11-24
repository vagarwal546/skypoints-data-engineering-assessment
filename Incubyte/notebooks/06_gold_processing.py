# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold Analytical Layer
# MAGIC Joins member profiles with flattened redemption transactions to produce member-level loyalty analytics.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "skypoints_dev")
catalog = dbutils.widgets.get("catalog")

members_df = spark.table(f"{catalog}.gold.member_master")
redemptions_df = spark.table(f"{catalog}.silver.redemption_transactions")

# COMMAND ----------
redemption_agg_df = (
    redemptions_df
    .groupBy("member_id")
    .agg(
        F.count("transaction_id").alias("total_redemptions"),
        F.sum("miles_redeemed").alias("total_miles_redeemed"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("miles_redeemed")).otherwise(0)).alias("completed_miles"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("miles_redeemed")).otherwise(0)).alias("pending_miles"),
        F.max("transaction_date").alias("last_redemption_date")
    )
)

analytics_df = (
    members_df.alias("m")
    .join(redemption_agg_df.alias("r"), on="member_id", how="left")
    .select(
        F.col("m.member_id"),
        F.col("m.member_name"),
        F.col("m.country"),
        F.col("m.tier_code"),
        F.col("m.age"),
        F.col("m.stale_member"),
        F.coalesce(F.col("r.total_redemptions"), F.lit(0)).alias("total_redemptions"),
        F.coalesce(F.col("r.total_miles_redeemed"), F.lit(0)).alias("total_miles_redeemed"),
        F.coalesce(F.col("r.completed_miles"), F.lit(0)).alias("completed_miles"),
        F.coalesce(F.col("r.pending_miles"), F.lit(0)).alias("pending_miles"),
        F.col("r.last_redemption_date"),
        F.current_timestamp().alias("refreshed_at")
    )
)

(
    analytics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.gold.member_redemption_analytics")
)

display(analytics_df)