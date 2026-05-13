# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data exploration
# MAGIC
# MAGIC Quick sanity checks before training. Designed to be the *first* thing a
# MAGIC new team member opens after cloning the Git folder.

# COMMAND ----------
CATALOG = "mbcl_catalog"
SCHEMA = "th_air_quality_forecast"

# COMMAND ----------
# MAGIC %md ## Coverage — every ward, every day

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ward_id, ward_name, COUNT(*) AS days, MIN(ds) AS first_day, MAX(ds) AS last_day
# MAGIC FROM mbcl_catalog.th_air_quality_forecast.gold_ward_daily_ts
# MAGIC GROUP BY ward_id, ward_name
# MAGIC ORDER BY ward_id

# COMMAND ----------
# MAGIC %md ## Distribution check — PM2.5 by ward

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ward_name,
# MAGIC        ROUND(AVG(y), 2) AS mean_pm25,
# MAGIC        ROUND(STDDEV(y), 2) AS sd_pm25,
# MAGIC        ROUND(MAX(y), 2) AS max_pm25
# MAGIC FROM mbcl_catalog.th_air_quality_forecast.gold_ward_daily_ts
# MAGIC GROUP BY ward_name
# MAGIC ORDER BY mean_pm25 DESC

# COMMAND ----------
# MAGIC %md ## Subscription health

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ward_id,
# MAGIC   COUNT(*) AS active_subs,
# MAGIC   COUNT(DISTINCT resident_id) AS distinct_residents,
# MAGIC   AVG(daqi_threshold) AS mean_threshold
# MAGIC FROM mbcl_catalog.th_air_quality_forecast.gold_active_subscriptions
# MAGIC GROUP BY ward_id
# MAGIC ORDER BY active_subs DESC
