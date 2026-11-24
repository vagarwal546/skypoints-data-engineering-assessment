# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC Ingests raw country CSVs and partner JSON redemption feeds into Bronze Delta tables without mutating source values.

# COMMAND ----------
from datetime import datetime
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "skypoints_dev")
dbutils.widgets.text("source_path", "/Volumes/skypoints_dev/bronze/landing_files")
dbutils.widgets.text("batch_id", datetime.utcnow().strftime("%Y%m%d%H%M%S"))

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------
def ingest_country_csv(file_name: str, country_code: str, column_map: dict):
    """Reads a country CSV as raw strings and aligns columns to the Bronze landing schema."""
    full_path = f"{source_path}/{file_name}"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .csv(full_path)
    )

    select_exprs = []
    target_cols = [
        "member_id", "member_name", "dob", "enrollment_date", "flight_date",
        "tier_code", "member_type", "agent_name", "state", "country", "is_active"
    ]

    for col_name in target_cols:
        if col_name in column_map and column_map[col_name] in df.columns:
            select_exprs.append(F.col(f"`{column_map[col_name]}`").cast("string").alias(col_name))
        elif col_name == "country":
            select_exprs.append(F.lit(country_code).alias("country"))
        else:
            select_exprs.append(F.lit(None).cast("string").alias(col_name))

    return (
        df.select(*select_exprs)
        .withColumn("source_file_name", F.lit(file_name))
        .withColumn("source_country", F.lit(country_code))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

# COMMAND ----------
# Column mappings for each country's specific schema
usa_map = {
    "member_id": "ID",
    "member_name": "Name",
    "tier_code": "TierCode",
    "enrollment_date": "EnrollmentDate",
    "flight_date": "FlightDate"
}

aus_map = {
    "member_id": "Unique ID",
    "member_name": "Member Name",
    "tier_code": "Tier Type",
    "dob": "Date of Birth",
    "enrollment_date": "Date of Enrollment",
    "flight_date": "Date of Flight"
}

ind_map = {
    "member_id": "ID",
    "member_name": "Name",
    "dob": "DOB",
    "tier_code": "TierCode",
    "enrollment_date": "EnrollmentDate",
    "member_type": "Individual or Corporate",
    "flight_date": "Flight Date"
}

usa_df = ingest_country_csv("usa.csv", "USA", usa_map)
aus_df = ingest_country_csv("australia.csv", "AUS", aus_map)
ind_df = ingest_country_csv("india.csv", "IND", ind_map)

bronze_members_df = usa_df.unionByName(aus_df).unionByName(ind_df)

(
    bronze_members_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.bronze.member_profiles_raw")
)

print(f"Ingested {bronze_members_df.count()} raw member records for batch_id={batch_id}")

# COMMAND ----------
# Ingest raw JSON redemption feed as raw text payload
json_path = f"{source_path}/redemption.json"
raw_json_df = (
    spark.read.text(json_path, wholetext=True)
    .select(
        F.col("value").alias("raw_payload"),
        F.lit("redemption.json").alias("source_file_name"),
        F.lit(batch_id).alias("batch_id"),
        F.current_timestamp().alias("ingestion_timestamp")
    )
)

(
    raw_json_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.bronze.redemption_raw")
)

print(f"Ingested raw redemption JSON payload for batch_id={batch_id}")