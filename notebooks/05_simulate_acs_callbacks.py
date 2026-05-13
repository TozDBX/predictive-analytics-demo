# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Simulate Azure Communication Services callbacks
# MAGIC
# MAGIC In production, the Azure Function that fronts ACS writes back into
# MAGIC `gold_alerts_audit` with the message ID and delivery status. For the
# MAGIC demo we generate those callbacks ourselves so the live ops dashboard
# MAGIC has a feed to render.
# MAGIC
# MAGIC Run this once before the demo (or in a loop during) to populate the
# MAGIC audit table with realistic delivery events.

# COMMAND ----------
import time
import uuid
import random
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta

CATALOG = "mbcl_catalog"
SCHEMA = "th_air_quality_forecast"

dbutils.widgets.text("batch_size", "60", "Alerts per batch")
dbutils.widgets.text("batches", "1", "Number of batches (each ~30s)")
dbutils.widgets.text("delivery_failure_rate", "0.04", "Probability of FAILED status")

BATCH = int(dbutils.widgets.get("batch_size"))
N_BATCHES = int(dbutils.widgets.get("batches"))
FAIL_RATE = float(dbutils.widgets.get("delivery_failure_rate"))

# COMMAND ----------
# MAGIC %md ## 1. Ensure audit table exists

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_alerts_audit (
      audit_id STRING,
      alert_key STRING,
      acs_message_id STRING,
      delivery_status STRING,        -- DELIVERED | FAILED | PENDING
      delivery_ts TIMESTAMP,
      latency_ms INT,
      failure_reason STRING,
      sent_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (delivery_status)
    """
)

# COMMAND ----------
# MAGIC %md ## 2. Pull the alerts that haven't been audited yet

# COMMAND ----------
unfired = (
    spark.table(f"{CATALOG}.{SCHEMA}.gold_alerts").alias("a")
    .join(
        spark.table(f"{CATALOG}.{SCHEMA}.gold_alerts_audit").select("alert_key").alias("au"),
        on="alert_key",
        how="left_anti",
    )
    .orderBy(F.rand())  # so we don't always pick the same wards first
    .limit(BATCH * N_BATCHES)
    .cache()
)
print(f"Unaudited alerts available to fire: {unfired.count()}")

# COMMAND ----------
# MAGIC %md ## 3. Fire batches with a short pause between them

# COMMAND ----------
audit_schema = T.StructType([
    T.StructField("audit_id", T.StringType()),
    T.StructField("alert_key", T.StringType()),
    T.StructField("acs_message_id", T.StringType()),
    T.StructField("delivery_status", T.StringType()),
    T.StructField("delivery_ts", T.TimestampType()),
    T.StructField("latency_ms", T.IntegerType()),
    T.StructField("failure_reason", T.StringType()),
    T.StructField("sent_at", T.TimestampType()),
])

unfired_keys = [r["alert_key"] for r in unfired.select("alert_key").collect()]
print(f"Will dispatch {len(unfired_keys)} alerts across {N_BATCHES} batch(es).")

batches = [unfired_keys[i:i + BATCH] for i in range(0, len(unfired_keys), BATCH)]
total_written = 0

for i, batch_keys in enumerate(batches, start=1):
    rows = []
    now = datetime.utcnow()
    for k in batch_keys:
        sent_at = now - timedelta(milliseconds=random.randint(50, 800))
        # Simulate realistic carrier latency: median ~600ms, fat tail to 4s.
        latency = int(random.lognormvariate(6.4, 0.5))
        delivery_ts = sent_at + timedelta(milliseconds=latency)
        is_fail = random.random() < FAIL_RATE
        status = "FAILED" if is_fail else "DELIVERED"
        reason = random.choice(["UNKNOWN_NUMBER", "OPT_OUT_DETECTED", "CARRIER_REJECT"]) if is_fail else None
        rows.append((
            str(uuid.uuid4()),
            k,
            f"acs_{uuid.uuid4().hex[:16]}" if not is_fail else None,
            status,
            delivery_ts,
            latency,
            reason,
            sent_at,
        ))
    df = spark.createDataFrame(rows, schema=audit_schema)
    df.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_alerts_audit")
    total_written += len(rows)
    print(f"Batch {i}/{N_BATCHES}: wrote {len(rows)} audit rows (running total: {total_written})")
    if i < N_BATCHES:
        time.sleep(30)

# COMMAND ----------
# MAGIC %md ## 4. Quick sanity check

# COMMAND ----------
display(
    spark.sql(
        f"""
        SELECT delivery_status, COUNT(*) AS n,
               ROUND(AVG(latency_ms), 0) AS avg_latency_ms,
               MIN(delivery_ts) AS first_event,
               MAX(delivery_ts) AS last_event
        FROM {CATALOG}.{SCHEMA}.gold_alerts_audit
        GROUP BY delivery_status
        """
    )
)
