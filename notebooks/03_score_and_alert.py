# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Score champions, generate alerts, fan out to webhook
# MAGIC
# MAGIC Daily batch:
# MAGIC 1. Pull latest gold timeseries.
# MAGIC 2. For each ward, load the champion model from UC Model Registry and
# MAGIC    forecast the next `HORIZON_DAYS`.
# MAGIC 3. Map forecast PM2.5 → DEFRA DAQI band; flag breaches.
# MAGIC 4. Join breaches to opted-in residents whose `daqi_threshold` is met.
# MAGIC 5. Emit `gold_alerts` (idempotent on `alert_key`) + an outbound payload.
# MAGIC 6. POST to the configured webhook (or skip in `DRY_RUN`).

# COMMAND ----------
# MAGIC %pip install -q prophet==1.1.5 statsforecast==1.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import datetime as dt
import hashlib
import json

import mlflow
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "mbcl_catalog"
SCHEMA = "th_air_quality_forecast"
HORIZON_DAYS = 7

dbutils.widgets.text("dry_run", "true", "Compose payload but skip webhook POST when 'true'")
DRY_RUN = dbutils.widgets.get("dry_run")
WEBHOOK_URL = dbutils.secrets.get(scope="lbth-air-quality", key="webhook_url") if DRY_RUN == "false" else None

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------
# MAGIC %md ## DAQI band reference (DEFRA)

# COMMAND ----------
# Tower Hamlets aligns with DEFRA's Daily Air Quality Index — bands derived
# from the higher of PM2.5 / NO2 / O3. We forecast PM2.5 only here; NO2 and
# O3 are easy follow-ups via the same pattern.
DAQI_BANDS = [
    (0,  11.0, 1, "Low"),
    (12, 23.0, 2, "Low"),
    (23, 35.0, 3, "Low"),
    (35, 41.0, 4, "Moderate"),
    (41, 47.0, 5, "Moderate"),
    (47, 53.0, 6, "Moderate"),
    (53, 58.0, 7, "High"),
    (58, 64.0, 8, "High"),
    (64, 70.0, 9, "High"),
    (70, 1e9, 10, "Very High"),
]


def daqi_band(pm25: float):
    for lo, hi, idx, label in DAQI_BANDS:
        if pm25 <= hi:
            return idx, label
    return 10, "Very High"


@F.udf(returnType="struct<band:int,label:string>")
def daqi_udf(pm25):
    band, label = daqi_band(float(pm25 or 0))
    return {"band": band, "label": label}

# COMMAND ----------
# MAGIC %md ## 1. Forecast next 7 days per ward

# COMMAND ----------
champion = spark.table(f"{CATALOG}.{SCHEMA}.gold_ward_champion").toPandas()
ts = (
    spark.table(f"{CATALOG}.{SCHEMA}.gold_ward_daily_ts")
    .select("ward_id", "ds", "y")
    .toPandas()
    .sort_values(["ward_id", "ds"])
)

forecasts = []
today = dt.date.today()
future_index = pd.date_range(today, periods=HORIZON_DAYS, freq="D")

for _, row in champion.iterrows():
    ward = row["ward_id"]
    model_family = row["model"]
    train = ts[ts["ward_id"] == ward].copy()

    if model_family == "Prophet":
        from prophet import Prophet
        m = Prophet(weekly_seasonality=True, yearly_seasonality=True, uncertainty_samples=0)
        m.fit(train[["ds", "y"]])
        f = m.make_future_dataframe(periods=HORIZON_DAYS, include_history=False)
        yhat = m.predict(f)["yhat"].to_numpy()
    elif model_family == "Keras":
        import tensorflow as tf
        from tensorflow import keras
        tf.random.set_seed(42)
        LAG = 28
        y = train["y"].to_numpy().astype("float32")
        X, Y = [], []
        for i in range(LAG, len(y) - HORIZON_DAYS + 1):
            X.append(y[i - LAG:i]); Y.append(y[i:i + HORIZON_DAYS])
        import numpy as _np
        X, Y = _np.array(X), _np.array(Y)
        m = keras.Sequential([
            keras.layers.Input(shape=(LAG,)),
            keras.layers.Dense(64, activation="relu"),
            keras.layers.Dropout(0.1),
            keras.layers.Dense(32, activation="relu"),
            keras.layers.Dense(HORIZON_DAYS),
        ])
        m.compile(optimizer="adam", loss="mae")
        m.fit(X, Y, epochs=30, batch_size=32, verbose=0)
        yhat = m.predict(y[-LAG:].reshape(1, -1), verbose=0).flatten()
    elif model_family in ("AutoARIMA", "AutoETS"):
        from statsforecast import StatsForecast
        from statsforecast.models import AutoARIMA, AutoETS
        models = {"AutoARIMA": AutoARIMA(season_length=7), "AutoETS": AutoETS(season_length=7)}
        sf = StatsForecast(models=[models[model_family]], freq="D")
        df = train.assign(unique_id=ward).rename(columns={"y": "y"})[["unique_id", "ds", "y"]]
        sf.fit(df)
        fc = sf.predict(h=HORIZON_DAYS).reset_index()
        yhat = fc[model_family].to_numpy()
    else:  # SeasonalNaive
        last7 = train["y"].tail(7).to_numpy()
        yhat = list(last7) * (HORIZON_DAYS // 7 + 1)
        yhat = yhat[:HORIZON_DAYS]

    for d, v in zip(future_index, yhat):
        forecasts.append({"ward_id": ward, "ds": d.date(), "yhat_pm25": float(v), "model": model_family})

forecast_sdf = spark.createDataFrame(pd.DataFrame(forecasts))
forecast_sdf = forecast_sdf.withColumn("daqi", daqi_udf("yhat_pm25"))
(
    forecast_sdf
    .write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_ward_forecast")
)
display(forecast_sdf.orderBy("ward_id", "ds"))

# COMMAND ----------
# MAGIC %md ## 2. Find breaches and join residents

# COMMAND ----------
breaches = forecast_sdf.filter("daqi.band >= 4")  # Moderate+
subs = spark.table(f"{CATALOG}.{SCHEMA}.gold_active_subscriptions")

alerts = (
    breaches.alias("b")
    .join(subs.alias("s"), F.col("b.ward_id") == F.col("s.ward_id"), "inner")
    .filter(F.col("b.daqi.band") >= F.col("s.daqi_threshold"))
    .select(
        F.col("s.subscription_id"),
        F.col("s.resident_id"),
        F.col("b.ward_id"),
        F.col("b.ds").alias("forecast_date"),
        F.col("b.yhat_pm25"),
        F.col("b.daqi.band").alias("daqi_band"),
        F.col("b.daqi.label").alias("daqi_label"),
        F.col("s.phone_e164_raw").alias("phone_e164"),
        F.col("s.phone_e164_masked").alias("phone_masked"),
        F.col("s.preferred_language"),
    )
    .withColumn(
        "alert_key",
        F.sha2(F.concat_ws("|", "subscription_id", "ward_id", "forecast_date", "daqi_band"), 256),
    )
    .withColumn("created_at", F.current_timestamp())
)

# Idempotent merge — never emit the same alert twice.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_alerts (
      alert_key STRING,
      subscription_id STRING,
      resident_id STRING,
      ward_id STRING,
      forecast_date DATE,
      yhat_pm25 DOUBLE,
      daqi_band INT,
      daqi_label STRING,
      phone_e164 STRING,
      phone_masked STRING,
      preferred_language STRING,
      created_at TIMESTAMP
    ) USING DELTA
    """
)

(
    alerts.createOrReplaceTempView("new_alerts")
)
spark.sql(
    f"""
    MERGE INTO {CATALOG}.{SCHEMA}.gold_alerts t
    USING new_alerts s
    ON t.alert_key = s.alert_key
    WHEN NOT MATCHED THEN INSERT *
    """
)

emitted = spark.sql(
    f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.gold_alerts
    WHERE created_at >= current_timestamp() - INTERVAL 5 MINUTES
    """
)
display(emitted)

# COMMAND ----------
# MAGIC %md ## 3. Webhook fan-out (or dry run)

# COMMAND ----------
def render_message(row, lang: str = "en") -> str:
    base = (
        f"LBTH air quality alert for {row['ward_id']}: "
        f"DAQI band {row['daqi_band']} ({row['daqi_label']}) forecast for {row['forecast_date']}. "
        f"Vulnerable residents: avoid prolonged exposure outdoors. lbth.gov/airquality"
    )
    if lang == "bn":
        return f"[BN] {base}"
    return base


payload = [
    {
        "alert_key": r["alert_key"],
        "to": r["phone_e164"],
        "to_masked": r["phone_masked"],
        "ward_id": r["ward_id"],
        "forecast_date": str(r["forecast_date"]),
        "daqi": {"band": r["daqi_band"], "label": r["daqi_label"]},
        "body": render_message(r, r["preferred_language"] or "en"),
    }
    for r in emitted.collect()
]

print(f"Composed {len(payload)} alert messages.")
print(json.dumps(payload[:2], indent=2, default=str))

if DRY_RUN == "true":
    print("DRY_RUN=true → not POSTing. Flip the job parameter to 'false' to enable.")
else:
    import requests
    resp = requests.post(
        WEBHOOK_URL,
        json={"alerts": payload, "source": "th_air_quality_daily"},
        timeout=10,
    )
    print(f"Webhook → {resp.status_code}")
    resp.raise_for_status()

# COMMAND ----------
# MAGIC %md ## 4. Audit log
# MAGIC The webhook target (Azure Function) is responsible for writing the
# MAGIC ACS message ID + delivery status into `gold_alerts_audit`. From the
# MAGIC Databricks side, we have proof of *intent* (the `gold_alerts` table) and
# MAGIC the webhook response — both queryable by the team.
