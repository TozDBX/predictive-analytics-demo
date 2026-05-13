"""Lakeflow Spark Declarative Pipeline — Tower Hamlets air quality batch.

Bronze: raw CSV readings + residents + subscriptions arriving in the
shared UC volume `mbcl_catalog.th_hub.raw_csv/` (the existing Intelligence
Hub volume — we share the volume so we don't double-upload data).

Silver: typed, quality-flagged, deduplicated.

Gold: per-ward daily timeseries ready for forecasting + the joined
resident subscription dataset used by the alerting job.

Run as a triggered (batch) Lakeflow Spark Declarative Pipeline.
Auto Loader uses Trigger.AvailableNow under the hood — see
https://learn.microsoft.com/en-us/azure/databricks/ldp/load.

Uses the new `pyspark.pipelines` API (replaces the legacy `dlt` module).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "mbcl_catalog"
SCHEMA = "th_air_quality_forecast"
RAW_VOL = "/Volumes/mbcl_catalog/th_hub/raw_csv"

# ---------------------------------------------------------------------------
# Bronze — Auto Loader, schema inferred + persisted in UC
# ---------------------------------------------------------------------------

@dp.table(
    name="bronze_readings",
    comment="Raw daily air quality readings landed from sensor exports.",
    table_properties={"quality": "bronze"},
)
def bronze_readings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{RAW_VOL}/_schemas/readings")
        .option("header", "true")
        .load(f"{RAW_VOL}/air_quality_readings*.csv")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )


@dp.table(
    name="bronze_residents",
    comment="Resident register (synthetic).",
    table_properties={"quality": "bronze"},
)
def bronze_residents():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{RAW_VOL}/_schemas/residents")
        .option("header", "true")
        .load(f"{RAW_VOL}/residents*.csv")
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_subscriptions",
    comment="Resident → ward SMS opt-in subscriptions (synthetic).",
    table_properties={"quality": "bronze"},
)
def bronze_subscriptions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{RAW_VOL}/_schemas/subscriptions")
        .option("header", "true")
        .load(f"{RAW_VOL}/subscriptions*.csv")
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Silver — typed, quality-flagged
# ---------------------------------------------------------------------------

@dp.table(
    name="silver_readings",
    comment="Cleaned, typed sensor readings — one row per ward per day.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_ward", "ward_id IS NOT NULL")
@dp.expect_or_drop("valid_date", "reading_date IS NOT NULL")
@dp.expect("plausible_pm25", "pm25 BETWEEN 0 AND 500")
@dp.expect("plausible_no2", "no2 BETWEEN 0 AND 500")
def silver_readings():
    # Source CSV column names follow the existing Intelligence Hub convention
    # (pm25_ugm3, no2_ugm3, o3_ugm3). Normalise here.
    return (
        spark.readStream.table("bronze_readings")
        .select(
            F.col("ward_id").cast("string").alias("ward_id"),
            F.col("ward_name").cast("string").alias("ward_name"),
            F.to_date("reading_date").alias("reading_date"),
            F.col("pm25_ugm3").cast("double").alias("pm25"),
            F.col("no2_ugm3").cast("double").alias("no2"),
            F.col("o3_ugm3").cast("double").alias("o3"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["ward_id", "reading_date"])
    )


@dp.table(
    name="silver_residents",
    comment="Resident register, typed; phone numbers retained as-is for masking in gold.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_resident", "resident_id IS NOT NULL")
def silver_residents():
    return (
        spark.readStream.table("bronze_residents")
        .select(
            F.col("resident_id").cast("string").alias("resident_id"),
            F.col("ward_id").cast("string").alias("ward_id"),
            F.col("phone_e164").cast("string").alias("phone_e164"),
            F.col("preferred_language").cast("string").alias("preferred_language"),
            F.col("_ingested_at"),
        )
    )


@dp.table(
    name="silver_subscriptions",
    comment="Active SMS opt-ins per resident, with sensitivity threshold.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_link", "resident_id IS NOT NULL AND ward_id IS NOT NULL")
@dp.expect("known_status", "opt_in_status IN ('ACTIVE','PAUSED','REVOKED')")
def silver_subscriptions():
    return (
        spark.readStream.table("bronze_subscriptions")
        .select(
            F.col("subscription_id").cast("string").alias("subscription_id"),
            F.col("resident_id").cast("string").alias("resident_id"),
            F.col("ward_id").cast("string").alias("ward_id"),
            F.col("opt_in_status").cast("string").alias("opt_in_status"),
            F.col("daqi_threshold").cast("int").alias("daqi_threshold"),
            F.to_timestamp("opt_in_at").alias("opt_in_at"),
        )
    )


# ---------------------------------------------------------------------------
# Gold — analysis-ready (materialized views, batch read of silver tables)
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold_ward_daily_ts",
    comment="Per-ward daily timeseries ready for forecasting (Prophet `ds`/`y` convention).",
    table_properties={"quality": "gold", "forecasting.target": "pm25"},
    partition_cols=["ward_id"],
)
def gold_ward_daily_ts():
    return (
        spark.read.table("silver_readings")
        .select(
            F.col("ward_id"),
            F.col("ward_name"),
            F.col("reading_date").alias("ds"),
            F.col("pm25").alias("y"),
            F.col("no2"),
            F.col("o3"),
        )
        .orderBy("ward_id", "ds")
    )


@dp.materialized_view(
    name="gold_active_subscriptions",
    comment="Joined view: opted-in residents per ward with masked contact for analysts.",
    table_properties={"quality": "gold"},
)
def gold_active_subscriptions():
    subs = spark.read.table("silver_subscriptions").filter("opt_in_status = 'ACTIVE'")
    res = spark.read.table("silver_residents")
    return (
        subs.join(res, on=["resident_id", "ward_id"], how="inner")
        .select(
            "subscription_id",
            "resident_id",
            "ward_id",
            "daqi_threshold",
            "preferred_language",
            F.col("phone_e164").alias("phone_e164_raw"),
            F.regexp_replace("phone_e164", r"(\+\d{2}\s?\d{4}\s?\d{3})\d{3}", r"$1***").alias("phone_e164_masked"),
        )
    )
