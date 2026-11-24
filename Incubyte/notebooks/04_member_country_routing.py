# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Member Deduplication & Country Routing
# MAGIC Applies "Latest Record Wins", detects country changes, updates `gold.member_master`, and syncs country-specific tables.

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "skypoints_dev")
catalog = dbutils.widgets.get("catalog")

COUNTRY_TABLE_MAP = {
    "USA": f"{catalog}.gold.member_usa",
    "IND": f"{catalog}.gold.member_india",
    "AUS": f"{catalog}.gold.member_australia",
    "PHL": f"{catalog}.gold.member_philippines",
    "CAN": f"{catalog}.gold.member_canada",
}

# COMMAND ----------
# 1. Deduplicate Silver records: Latest record wins per member_id
silver_df = spark.table(f"{catalog}.silver.member_profiles")

window_spec = Window.partitionBy("member_id").orderBy(
    F.col("ingestion_timestamp").desc(),
    F.col("batch_id").desc(),
    F.col("flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

latest_members_df = (
    silver_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn", "dq_warnings", "source_file_name", "batch_id", "ingestion_timestamp")
    .withColumn("last_updated_at", F.current_timestamp())
)

# COMMAND ----------
# 2. Detect members who moved countries by comparing against current gold.member_master
master_table_name = f"{catalog}.gold.member_master"
current_master_df = spark.table(master_table_name).select(
    F.col("member_id"),
    F.col("country").alias("old_country")
)

moved_members_df = (
    latest_members_df.alias("new")
    .join(current_master_df.alias("old"), on="member_id", how="inner")
    .filter(F.col("new.country") != F.col("old.old_country"))
    .select("member_id", "old_country", F.col("new.country").alias("new_country"))
)

# Remove moved members from their previous country tables
for old_country, target_table in COUNTRY_TABLE_MAP.items():
    ids_to_remove = moved_members_df.filter(F.col("old_country") == old_country).select("member_id")
    if not ids_to_remove.isEmpty():
        DeltaTable.forName(spark, target_table).alias("t").merge(
            ids_to_remove.alias("s"),
            "t.member_id = s.member_id"
        ).whenMatchedDelete().execute()

# COMMAND ----------
# 3. Upsert into Gold Member Master
DeltaTable.forName(spark, master_table_name).alias("t").merge(
    latest_members_df.alias("s"),
    "t.member_id = s.member_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------
# 4. Route and upsert into country-specific Gold tables
for country_code, target_table in COUNTRY_TABLE_MAP.items():
    country_df = latest_members_df.filter(F.col("country") == country_code)
    if not country_df.isEmpty():
        DeltaTable.forName(spark, target_table).alias("t").merge(
            country_df.alias("s"),
            "t.member_id = s.member_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"Synced {country_df.count()} records to {target_table}")