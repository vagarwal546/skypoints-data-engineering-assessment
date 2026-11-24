# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformation
# MAGIC Cleanses strings, normalizes country codes, parses multi-format dates safely, and computes Age and Stale_Member.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "skypoints_dev")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("stale_threshold_days", "90")

catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")
stale_threshold_days = int(dbutils.widgets.get("stale_threshold_days"))

# COMMAND ----------
raw_df = spark.table(f"{catalog}.bronze.member_profiles_raw")
if batch_id:
    raw_df = raw_df.filter(F.col("batch_id") == batch_id)

# COMMAND ----------
def parse_multi_format_date(col_name: str):
    """
    Handles:
      - Literal 'NULL' / empty strings -> None
      - ISO format: yyyy-MM-dd (Australia)
      - Slash format: M/d/yyyy (India)
      - Compact 7-8 digit USA format: 6152022 -> 06152022 (MMddyyyy)
      - Compact ISO format: yyyyMMdd (Flat file)
    Uses try_to_date so invalid calendar dates like '2021-13-13' return NULL instead of crashing.
    """
    cleaned = F.when(
        F.upper(F.trim(F.col(col_name))).isin("", "NULL", "NONE"),
        F.lit(None)
    ).otherwise(F.trim(F.col(col_name)))

    padded_usa = F.when(F.length(cleaned) == 7, F.lpad(cleaned, 8, "0")).otherwise(cleaned)

    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date({col_name}, 'M/d/yyyy')"),
        F.try_to_date(padded_usa, F.lit("MMddyyyy")),
        F.try_to_date(cleaned, F.lit("yyyyMMdd")),
        F.try_to_date(cleaned, F.lit("ddMMyyyy"))
    )

# COMMAND ----------
country_normalization = F.create_map(
    F.lit("USA"), F.lit("USA"),
    F.lit("US"), F.lit("USA"),
    F.lit("IND"), F.lit("IND"),
    F.lit("IN"), F.lit("IND"),
    F.lit("AUS"), F.lit("AUS"),
    F.lit("AU"), F.lit("AUS"),
    F.lit("PHIL"), F.lit("PHL"),
    F.lit("CAN"), F.lit("CAN")
)

transformed_df = (
    raw_df
    .withColumn("member_id", F.trim(F.col("member_id")))
    .withColumn("member_name", F.trim(F.col("member_name")))
    .withColumn("tier_code", F.upper(F.trim(F.col("tier_code"))))
    .withColumn("member_type", F.upper(F.trim(F.col("member_type"))))
    .withColumn("country_raw", F.upper(F.trim(F.col("country"))))
    .withColumn("country", F.coalesce(country_normalization[F.col("country_raw")], F.col("country_raw")))
    # Retain raw date strings for downstream DQ checks before overwriting
    .withColumn("dob_raw", F.col("dob"))
    .withColumn("enrollment_date_raw", F.col("enrollment_date"))
    .withColumn("flight_date_raw", F.col("flight_date"))
    # Parse dates
    .withColumn("dob", parse_multi_format_date("dob"))
    .withColumn("enrollment_date", parse_multi_format_date("enrollment_date"))
    .withColumn("flight_date", parse_multi_format_date("flight_date"))
    # Accurate Age calculation accounting for whether birthday has passed this year
    .withColumn(
        "age",
        F.when(F.col("dob").isNull(), F.lit(None).cast("int"))
        .otherwise(
            F.year(F.current_date()) - F.year(F.col("dob"))
            - F.when(
                F.date_format(F.current_date(), "MMdd") < F.date_format(F.col("dob"), "MMdd"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )
    # Configurable Stale_Member flag (> 90 days since last flight)
    .withColumn(
        "stale_member",
        F.when(F.col("flight_date").isNull(), F.lit(None).cast("int"))
        .when(F.datediff(F.current_date(), F.col("flight_date")) > stale_threshold_days, F.lit(1))
        .otherwise(F.lit(0))
    )
)

# Write to a temporary staging view/table for Notebook 03 (Data Quality)
transformed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.silver.member_profiles_staged"
)

display(transformed_df.select("member_id", "member_name", "country", "dob", "enrollment_date", "flight_date", "age", "stale_member"))