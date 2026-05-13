# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Team collaboration runbook
# MAGIC
# MAGIC A live notebook for the LBTH data team to use as a reference once they
# MAGIC fork this project internally. Walks through:
# MAGIC
# MAGIC 1. How to clone the repo into your own Git folder.
# MAGIC 2. How to find your colleagues' MLflow runs in the shared experiment.
# MAGIC 3. How to add a new model family without touching the orchestration.
# MAGIC 4. How to promote a champion change through the bundle.

# COMMAND ----------
# MAGIC %md ## 1. Clone into your Git folder
# MAGIC
# MAGIC Workspace UI → **Workspace → Users → your.email → New → Git folder**
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | Git repo URL | `https://dev.azure.com/<org>/_git/lbth-data-platform` (or GitHub equivalent) |
# MAGIC | Branch | `feature/<your-ticket>-<slug>` |
# MAGIC | Path | `/Workspace/Users/<your.email>/air-quality-forecasting` |
# MAGIC
# MAGIC Each engineer gets their own clone. Pull `develop` regularly. **Never**
# MAGIC commit directly to `main`.

# COMMAND ----------
# MAGIC %md ## 2. Find runs from across the team

# COMMAND ----------
import mlflow

EXPERIMENT_PATH = "/Shared/lbth/air_quality_forecasting/experiments/multi_model_bakeoff"
exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)

# All Prophet runs from the last 7 days, any teammate.
df = mlflow.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string="tags.model = 'Prophet'",
    max_results=200,
)
display(df[["tags.run_owner", "tags.ward_id", "tags.branch", "metrics.smape", "metrics.mae", "start_time"]].sort_values("start_time", ascending=False))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Filter recipes
# MAGIC
# MAGIC | I want… | `filter_string` |
# MAGIC |---------|-----------------|
# MAGIC | My runs only | `tags.run_owner = '<your.email>'` |
# MAGIC | A specific ward | `tags.ward_id = 'TH04'` |
# MAGIC | Bake-off runs only (skip experiments) | `tags.run_purpose = 'bakeoff'` |
# MAGIC | Runs from a feature branch | `tags.branch = 'feature/AQF-42-add-tbats'` |

# COMMAND ----------
# MAGIC %md ## 3. Add a new model family
# MAGIC
# MAGIC In `02_train_and_compare`, add to the `MODELS` dict:
# MAGIC
# MAGIC ```python
# MAGIC MODELS["AutoTBATS"] = fit_statsforecast("AutoTBATS")
# MAGIC ```
# MAGIC
# MAGIC No other code change needed — the runner is model-agnostic. Open a PR
# MAGIC against `develop`, link to the MLflow comparison view in the description,
# MAGIC tag the ML lead for review.

# COMMAND ----------
# MAGIC %md ## 4. Promote a champion change
# MAGIC
# MAGIC Champions are written into UC Model Registry by the daily job. To make
# MAGIC a champion change *sticky* across runs, change the registry alias from
# MAGIC the bundle:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC The bundle target `prod` deploys to the LBTH workspace; deployment
# MAGIC permissions are scoped to the `lbth-data-platform-deployers` group.
