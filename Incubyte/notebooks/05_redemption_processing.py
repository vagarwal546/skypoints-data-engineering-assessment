# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Redemption JSON Processing
# MAGIC Flattens nested partner airline redemption transactions and performs an idempotent Delta MERGE on `transaction_id`.

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

dbutils.widgets.text("catalog", "skypoints_dev")
dbutils.widgets.text("batch_id", "")

catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------
# Explicit schema matching the partner JSON (note: "member id" has a space)
redemption_schema = StructType([
    StructField("member id", StringType(), True),
    StructField("feed_date", StringType(), True),
    StructField("redemptions", ArrayType(StructType([
        StructField("txn_id", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("partner", StringType(), True),
        StructField("miles_redeemed", LongType(), True),
        StructField("status", StringType(), True),
    ])), True)
])

raw_json_df = spark.table(f"{catalog}.bronze.redemption_raw")
if batch_id:
    raw_json_df = raw_json_df.filter(F.col("batch_id") == batch_id)

# COMMAND ----------
flattened_df = (
    raw_json_df
    .withColumn("parsed_array", F.from_json(F.col("raw_payload"), ArrayType(redemption_schema)))
    .withColumn("parsed", F.explode(F.col("parsed_array")))
    .withColumn("redemption", F.explode(F.col("parsed.redemptions")))
    .select(
        F.trim(F.col("redemption.txn_id")).alias("transaction_id"),
        F.trim(F.col("parsed.`member id`")).alias("member_id"),
        F.try_to_date(F.col("parsed.feed_date"), F.lit("yyyyMMdd")).alias("feed_date"),
        F.try_to_date(F.col("redemption.txn_date"), F.lit("yyyyMMdd")).alias("transaction_date"),
        F.trim(F.col("redemption.partner")).alias("partner"),
        F.col("redemption.miles_redeemed").cast("bigint").alias("miles_redeemed"),
        F.upper(F.trim(F.col("redemption.status"))).alias("status"),
        F.col("batch_id"),
        F.col("ingestion_timestamp")
    )
    .filter(F.col("transaction_id").isNotNull() & (F.col("miles_redeemed") > 0))
    .dropDuplicates(["transaction_id"])
)

# Idempotent MERGE into silver.redemption_transactions
target_table = f"{catalog}.silver.redemption_transactions"
DeltaTable.forName(spark, target_table).alias("t").merge(
    flattened_df.alias("s"),
    "t.transaction_id = s.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"Merged {flattened_df.count()} redemption transactions into {target_table}")