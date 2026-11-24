# Databricks notebook source
from datetime import date, datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType


def test_multi_format_date_parsing(spark):
    """Validates USA compact dates, India slash dates, ISO dates, literal NULL, and invalid dates."""
    data = [
        ("1", "6152022"),      # USA 7-digit MMddyyyy
        ("2", "12282021"),     # USA 8-digit MMddyyyy
        ("3", "12/1/1998"),    # India M/d/yyyy
        ("4", "2022-05-11"),   # Australia yyyy-MM-dd
        ("5", "2021-13-13"),   # Invalid calendar date -> NULL
        ("6", "NULL"),         # Literal NULL string -> NULL
    ]
    df = spark.createDataFrame(data, ["id", "raw_date"])

    cleaned = F.when(F.upper(F.trim(F.col("raw_date"))).isin("", "NULL", "NONE"), F.lit(None)).otherwise(F.trim(F.col("raw_date")))
    padded = F.when(F.length(cleaned) == 7, F.lpad(cleaned, 8, "0")).otherwise(cleaned)

    parsed_df = df.withColumn(
        "parsed_date",
        F.coalesce(
            F.expr("try_to_date(raw_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(raw_date, 'M/d/yyyy')"),
            F.try_to_date(padded, F.lit("MMddyyyy")),
        )
    )

    results = {row["id"]: row["parsed_date"] for row in parsed_df.collect()}
    assert results["1"] == date(2022, 6, 15)
    assert results["2"] == date(2021, 12, 28)
    assert results["3"] == date(1998, 12, 1)
    assert results["4"] == date(2022, 5, 11)
    assert results["5"] is None
    assert results["6"] is None


def test_latest_record_wins_country_movement(spark):
    """Ensures latest record wins when a member moves from USA to IND."""
    data = [
        ("223457", "Elena", "USA", "20240110", datetime(2024, 1, 10, 8, 0, 0)),
        ("223457", "Elena", "IND", "20240115", datetime(2024, 1, 15, 8, 0, 0)),
        ("223458", "Ravi", "IND", "20240110", datetime(2024, 1, 10, 8, 0, 0)),
    ]
    df = spark.createDataFrame(data, ["member_id", "member_name", "country", "batch_id", "ingestion_timestamp"])

    w = Window.partitionBy("member_id").orderBy(F.col("ingestion_timestamp").desc(), F.col("batch_id").desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    results = {row["member_id"]: row["country"] for row in latest.collect()}
    assert results["223457"] == "IND"
    assert results["223458"] == "IND"
    assert latest.count() == 2


def test_redemption_json_flattening(spark):
    """Validates flattening of nested redemptions and handling of 'member id' key with space."""
    raw_json = """
    {
      "member id": "223457",
      "feed_date": "20240115",
      "redemptions": [
        {"txn_id": "RX10091", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 12000, "status": "COMPLETED"},
        {"txn_id": "RX10092", "txn_date": "20240113", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "PENDING"}
      ]
    }
    """
    schema = StructType([
        StructField("member id", StringType(), True),
        StructField("feed_date", StringType(), True),
        StructField("redemptions", ArrayType(StructType([
            StructField("txn_id", StringType(), True),
            StructField("txn_date", StringType(), True),
            StructField("partner", StringType(), True),
            StructField("miles_redeemed", LongType(), True),
            StructField("status", StringType(), True),
        ])), True),
    ])

    df = spark.createDataFrame([(raw_json,)], ["raw_payload"])
    flattened = (
        df.withColumn("parsed", F.from_json(F.col("raw_payload"), schema))
        .withColumn("r", F.explode("parsed.redemptions"))
        .select(
            F.col("parsed.`member id`").alias("member_id"),
            F.col("r.txn_id").alias("transaction_id"),
            F.col("r.miles_redeemed").alias("miles_redeemed"),
            F.col("r.status").alias("status"),
        )
    )

    rows = flattened.orderBy("transaction_id").collect()
    assert len(rows) == 2
    assert rows[0]["member_id"] == "223457"
    assert rows[0]["transaction_id"] == "RX10091"
    assert rows[0]["miles_redeemed"] == 12000
    assert rows[1]["status"] == "PENDING"