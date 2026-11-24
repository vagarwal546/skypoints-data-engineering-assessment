# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Data Quality Validation & Quarantine
# MAGIC Splits staged records into valid Silver records and quarantined Rejected records with explicit failure reasons.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "skypoints_dev")
catalog = dbutils.widgets.get("catalog")

staged_df = spark.table(f"{catalog}.silver.member_profiles_staged")

# COMMAND ----------
VALID_TIERS = ["SLV", "GLD", "PLT"]
VALID_COUNTRIES = ["USA", "IND", "AUS", "PHL", "CAN"]
VALID_MEMBER_TYPES = ["I", "C"]

# Build array of hard rejection reasons
rejection_reasons = F.array_compact(F.array(
    F.when(F.col("member_id").isNull() | (F.col("member_id") == ""), F.lit("MISSING_MEMBER_ID")),
    F.when(F.col("member_name").isNull() | (F.col("member_name") == ""), F.lit("MISSING_MEMBER_NAME")),
    F.when(F.col("enrollment_date").isNull(), F.lit("INVALID_OR_MISSING_ENROLLMENT_DATE")),
    F.when(F.col("dob") > F.current_date(), F.lit("DOB_IN_FUTURE")),
    F.when(
        F.col("dob_raw").isNotNull()
        & (~F.upper(F.trim(F.col("dob_raw"))).isin("", "NULL", "NONE"))
        & F.col("dob").isNull(),
        F.lit("UNPARSEABLE_DOB")
    ),
    F.when(~F.col("country").isin(VALID_COUNTRIES), F.lit("UNKNOWN_COUNTRY"))
))

# Build array of non-blocking warnings
dq_warnings = F.array_compact(F.array(
    F.when(F.col("tier_code").isNotNull() & (~F.col("tier_code").isin(VALID_TIERS)), F.lit("UNKNOWN_TIER_CODE")),
    F.when(F.col("member_type").isNotNull() & (~F.col("member_type").isin(VALID_MEMBER_TYPES)), F.lit("INVALID_MEMBER_TYPE")),
    F.when(F.col("flight_date") > F.current_date(), F.lit("FLIGHT_DATE_IN_FUTURE")),
    F.when(F.col("agent_name").isNull(), F.lit("MISSING_AGENT_NAME"))
))

evaluated_df = (
    staged_df
    .withColumn("rejection_reasons", rejection_reasons)
    .withColumn("dq_warnings", dq_warnings)
)

# COMMAND ----------
valid_df = (
    evaluated_df
    .filter(F.size("rejection_reasons") == 0)
    .select(
        "member_id", "member_name", "dob", "enrollment_date", "flight_date",
        "tier_code", "member_type", "agent_name", "state", "country",
        "is_active", "age", "stale_member", "dq_warnings",
        "source_file_name", "batch_id", "ingestion_timestamp"
    )
)

rejected_df = (
    evaluated_df
    .filter(F.size("rejection_reasons") > 0)
    .select(
        "member_id", "member_name",
        F.col("dob_raw").alias("dob"),
        F.col("enrollment_date_raw").alias("enrollment_date"),
        F.col("flight_date_raw").alias("flight_date"),
        "tier_code", "country", "rejection_reasons",
        "source_file_name", "batch_id",
        F.current_timestamp().alias("rejected_at")
    )
)

valid_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver.member_profiles")
rejected_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver.rejected_members")

print(f"Valid records written: {valid_df.count()} | Quarantined records: {rejected_df.count()}")