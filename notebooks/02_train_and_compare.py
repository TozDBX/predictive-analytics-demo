# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Multi-model forecast bake-off
# MAGIC
# MAGIC Trains four families per ward in parallel via `applyInPandas`, logs every
# MAGIC run to a single shared MLflow experiment, picks a champion per ward by
# MAGIC backtest sMAPE, and registers the champion to UC Model Registry.
# MAGIC
# MAGIC Models compared:
# MAGIC
# MAGIC | Family | Library | Why it's here |
# MAGIC |--------|---------|---------------|
# MAGIC | **SeasonalNaive** | numpy | Honest baseline — anything we ship must beat this |
# MAGIC | **Prophet** | `prophet` | Easy to narrate, captures weekly + annual seasonality, holiday-aware |
# MAGIC | **AutoARIMA** | `statsforecast` | Classical, strong on stationary-ish series, fast |
# MAGIC | **AutoETS** | `statsforecast` | Strong on additive seasonality, good complement to ARIMA |
# MAGIC
# MAGIC Backed by the [Many Model Forecasting](https://github.com/databricks-industry-solutions/many-model-forecasting)
# MAGIC pattern. Swap in TimesFM / Chronos / Moirai later by adding another row to
# MAGIC the model registry dict — the rest of the notebook is model-agnostic.

# COMMAND ----------
# MAGIC %pip install -q prophet==1.1.5 statsforecast==1.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import os
import json
import datetime as dt
from typing import Callable

import mlflow
import mlflow.prophet  # module-scoped so train_one_ward doesn't shadow `mlflow`
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "mbcl_catalog"
SCHEMA = "th_air_quality_forecast"
HORIZON_DAYS = 7
BACKTEST_DAYS = 28  # last 28 days held out for backtest

# The bundle injects the experiment path so dev-mode prefixes (`[dev <user>]`)
# resolve correctly and runs land in the bundle-managed experiment.
dbutils.widgets.text(
    "shared_experiment",
    "/Shared/lbth/air_quality_forecasting/experiments/multi_model_bakeoff",
    "MLflow experiment path",
)
EXPERIMENT_PATH = dbutils.widgets.get("shared_experiment")
mlflow.set_experiment(EXPERIMENT_PATH)
print(f"Logging to experiment: {EXPERIMENT_PATH}")

# Tag every run so any teammate can filter to their work.
RUN_OWNER = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
)
GIT_COMMIT = os.environ.get("GIT_COMMIT", "local")
BRANCH = os.environ.get("GIT_BRANCH", "develop")

# COMMAND ----------
# MAGIC %md ## 1. Pull the gold timeseries

# COMMAND ----------
ts = (
    spark.table(f"{CATALOG}.{SCHEMA}.gold_ward_daily_ts")
    .select("ward_id", "ward_name", "ds", "y")
    .orderBy("ward_id", "ds")
)
ts.createOrReplaceTempView("ts")
display(ts.groupBy("ward_id").count().orderBy("ward_id"))

# COMMAND ----------
# MAGIC %md ## 2. Backtest split per ward

# COMMAND ----------
max_ds = ts.agg(F.max("ds")).first()[0]
cutoff = max_ds - dt.timedelta(days=BACKTEST_DAYS)
print(f"Train ≤ {cutoff} · Backtest ({cutoff} → {max_ds}]")

# COMMAND ----------
# MAGIC %md ## 3. Per-ward parallel training via applyInPandas
# MAGIC
# MAGIC Each `(ward_id, model_family)` pair becomes a Pandas group → one MLflow
# MAGIC run. The pattern scales linearly with cluster size.

# COMMAND ----------
def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    diff = np.abs(y_true - y_pred) / np.where(denom == 0, 1, denom)
    return float(np.mean(diff) * 100)


# Each fit_* returns (yhat_df, fitted_state). fitted_state is whatever the
# WardForecaster pyfunc needs at predict time — kept small and pickle-safe.

def fit_seasonal_naive(train: pd.DataFrame, horizon: int):
    season = 7
    tail = train["y"].tail(season).to_numpy()
    reps = int(np.ceil(horizon / season))
    yhat = np.tile(tail, reps)[:horizon]
    return pd.DataFrame({"yhat": yhat}), {"family": "SeasonalNaive", "train_tail": tail.tolist()}


def fit_prophet(train: pd.DataFrame, horizon: int):
    from prophet import Prophet
    # uncertainty_samples=0 disables the MCMC-style interval recomputation
    # that reads `stan_backend` — workaround for the serverless Prophet build.
    m = Prophet(
        weekly_seasonality=True,
        yearly_seasonality=True,
        daily_seasonality=False,
        uncertainty_samples=0,
    )
    m.fit(train.rename(columns={"ds": "ds", "y": "y"}))
    future = m.make_future_dataframe(periods=horizon, include_history=False)
    yhat = m.predict(future)[["yhat"]].reset_index(drop=True)
    return yhat, {
        "family": "Prophet",
        "frozen_forecast": yhat["yhat"].tolist(),
        "frozen_horizon": horizon,
    }


def fit_keras(train: pd.DataFrame, horizon: int):
    """Tiny Keras Sequential — 28-day lag MLP, direct multi-step head."""
    import tensorflow as tf
    from tensorflow import keras

    tf.random.set_seed(42)
    LAG = 28
    y = train["y"].to_numpy().astype("float32")

    X, Y = [], []
    for i in range(LAG, len(y) - horizon + 1):
        X.append(y[i - LAG:i])
        Y.append(y[i:i + horizon])
    X = np.array(X) if X else np.zeros((1, LAG), dtype="float32")
    Y = np.array(Y) if Y else np.zeros((1, horizon), dtype="float32")

    model = keras.Sequential([
        keras.layers.Input(shape=(LAG,)),
        keras.layers.Dense(64, activation="relu"),
        keras.layers.Dropout(0.1),
        keras.layers.Dense(32, activation="relu"),
        keras.layers.Dense(horizon),
    ])
    model.compile(optimizer="adam", loss="mae")
    model.fit(X, Y, epochs=30, batch_size=32, verbose=0)

    last_window = y[-LAG:].reshape(1, -1)
    yhat = model.predict(last_window, verbose=0).flatten().tolist()
    return pd.DataFrame({"yhat": yhat}), {
        "family": "Keras",
        "frozen_forecast": yhat,
        "frozen_horizon": horizon,
        "n_params": int(model.count_params()),
        "n_train_samples": int(len(X)),
    }


def fit_statsforecast(model_name: str) -> Callable:
    def _fit(train: pd.DataFrame, horizon: int):
        from statsforecast import StatsForecast
        from statsforecast.models import AutoARIMA, AutoETS
        models = {"AutoARIMA": AutoARIMA(season_length=7), "AutoETS": AutoETS(season_length=7)}
        sf = StatsForecast(models=[models[model_name]], freq="D", n_jobs=1)
        df = train.assign(unique_id="series").rename(columns={"ds": "ds", "y": "y"})[["unique_id", "ds", "y"]]
        sf.fit(df)
        fc = sf.predict(h=horizon).reset_index(drop=True)
        yhat = pd.DataFrame({"yhat": fc[model_name].to_numpy()})
        return yhat, {"family": model_name, "fitted": sf}
    return _fit


MODELS = {
    "SeasonalNaive": fit_seasonal_naive,
    "Prophet": fit_prophet,        # frozen-forecast pyfunc — sidesteps stan_backend issue
    "AutoARIMA": fit_statsforecast("AutoARIMA"),
    "AutoETS": fit_statsforecast("AutoETS"),
    "Keras": fit_keras,            # 28-day lag MLP, direct 7-day forecast head
}


# Single pyfunc wrapper that handles all four families. mlflow.pyfunc serialises
# this with cloudpickle; Prophet + StatsForecast both pickle cleanly.

class WardForecaster(mlflow.pyfunc.PythonModel):
    def __init__(self, state):
        self.state = state

    def predict(self, context, model_input):
        # model_input: pandas DataFrame with one column 'horizon'
        h = int(model_input["horizon"].iloc[0])
        family = self.state["family"]
        if family == "SeasonalNaive":
            tail = np.array(self.state["train_tail"])
            reps = int(np.ceil(h / len(tail)))
            return np.tile(tail, reps)[:h].tolist()
        if family in ("Prophet", "Keras"):
            forecast = self.state["frozen_forecast"]
            if h > len(forecast):
                forecast = forecast + [forecast[-1]] * (h - len(forecast))
            return forecast[:h]
        if family in ("AutoARIMA", "AutoETS"):
            sf = self.state["fitted"]
            fc = sf.predict(h=h).reset_index(drop=True)
            return fc[family].tolist()
        raise ValueError(f"Unknown family: {family}")

# COMMAND ----------
result_schema = T.StructType(
    [
        T.StructField("ward_id", T.StringType()),
        T.StructField("model", T.StringType()),
        T.StructField("smape_backtest", T.DoubleType()),
        T.StructField("mae_backtest", T.DoubleType()),
        T.StructField("run_id", T.StringType()),
    ]
)


def train_one_ward(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("ds").reset_index(drop=True)
    ward_id = pdf["ward_id"].iloc[0]
    train = pdf[pdf["ds"] <= pd.Timestamp(cutoff)]
    test = pdf[pdf["ds"] > pd.Timestamp(cutoff)]
    horizon = len(test)

    rows = []
    for model_name, fit_fn in MODELS.items():
        with mlflow.start_run(
            run_name=f"{ward_id}__{model_name}",
            tags={
                "model": model_name,
                "ward_id": ward_id,
                "run_owner": RUN_OWNER,
                "branch": BRANCH,
                "git_commit": GIT_COMMIT,
                "run_purpose": "bakeoff",
            },
        ) as run:
            mlflow.log_params(
                {"horizon_days": HORIZON_DAYS, "backtest_days": BACKTEST_DAYS, "n_train": len(train)}
            )
            try:
                preds, state = fit_fn(train[["ds", "y"]], horizon)
                yhat = preds["yhat"].to_numpy()[:horizon]
                ytrue = test["y"].to_numpy()[:horizon]
                sm = smape(ytrue, yhat)
                mae = float(np.mean(np.abs(ytrue - yhat)))
                mlflow.log_metrics({"smape": sm, "mae": mae})
                # Log as an MLflow 3 LoggedModel so the experiment "Models" tab
                # tracks versions per (ward × family) over time. Prophet has its
                # own flavour because cloudpickle can't serialise Prophet's
                # internal stan_backend after .fit().
                logged_model_name = f"ward_{ward_id.lower()}_{model_name.lower()}"
                # All families now go through the same pyfunc wrapper.
                # Prophet's state is JSON-serialised inside fit_prophet, so
                # cloudpickle never sees the fitted Prophet object.
                mlflow.pyfunc.log_model(
                    name=logged_model_name,
                    python_model=WardForecaster(state),
                    input_example=pd.DataFrame({"horizon": [HORIZON_DAYS]}),
                )
                rows.append((ward_id, model_name, sm, mae, run.info.run_id))
            except Exception as exc:  # log + continue so one model failure doesn't kill the ward
                mlflow.set_tag("error", str(exc)[:500])
                rows.append((ward_id, model_name, float("inf"), float("inf"), run.info.run_id))

    return pd.DataFrame(rows, columns=["ward_id", "model", "smape_backtest", "mae_backtest", "run_id"])


ts_pd = ts.toPandas()
ts_pd["ds"] = pd.to_datetime(ts_pd["ds"])
chunks = []
for ward_id, group in ts_pd.groupby("ward_id"):
    g = group.copy()
    g["ward_id"] = ward_id  # ensure column present in case pandas excluded it
    chunks.append(train_one_ward(g))
results = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(
    columns=["ward_id", "model", "smape_backtest", "mae_backtest", "run_id"]
)
results_sdf = spark.createDataFrame(results)
results_sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_model_bakeoff")
display(results_sdf.orderBy("ward_id", "smape_backtest"))

# COMMAND ----------
# MAGIC %md ## 4. Pick a champion per ward

# COMMAND ----------
champion_window = (
    results_sdf
    .withColumn(
        "rnk",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .partitionBy("ward_id")
            .orderBy(F.col("smape_backtest").asc())
        ),
    )
    .filter("rnk = 1")
    .drop("rnk")
)
champion_window.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_ward_champion")
display(champion_window)

# COMMAND ----------
# MAGIC %md ## 5. Register champions to UC Model Registry
# MAGIC
# MAGIC We register **one model per ward** under the alias `champion@ward_<id>`
# MAGIC so the scoring job can resolve the right model dynamically.

# COMMAND ----------
client = mlflow.MlflowClient()
mlflow.set_registry_uri("databricks-uc")

for row in champion_window.collect():
    # Artifact path inside the run matches the LoggedModel name we set above.
    artifact = f"ward_{row['ward_id'].lower()}_{row['model'].lower()}"
    model_uri = f"runs:/{row['run_id']}/{artifact}"
    name = f"{CATALOG}.{SCHEMA}.ward_{row['ward_id'].lower()}_champion"
    try:
        mv = mlflow.register_model(model_uri=model_uri, name=name)
        client.set_registered_model_alias(name=name, alias="champion", version=mv.version)
        client.set_model_version_tag(name=name, version=mv.version, key="model_family", value=row["model"])
        client.set_model_version_tag(name=name, version=mv.version, key="smape_backtest", value=str(row["smape_backtest"]))
    except Exception as exc:
        print(f"Skipping registry for {name}: {exc}")

print("Champions registered.")

# COMMAND ----------
# MAGIC %md ## 6. What to show in the demo
# MAGIC
# MAGIC 1. **Experiment → Runs tab** → filter by `tags.run_purpose = 'bakeoff'` → group by `ward_id` → sort by `smape`. "Prophet wins on traffic-heavy wards, ETS wins on quieter ones."
# MAGIC 2. **Experiment → Models tab** → 80 LoggedModels named `ward_<id>_<family>`. Each daily run adds a new version, so "model drift over time" becomes a click.
# MAGIC 3. **`gold_model_bakeoff` table** → one row per (ward × model). Lineage view shows descent from `gold_ward_daily_ts`.
# MAGIC 4. **`gold_ward_champion` table** → 20 rows, one per ward, with the winning model name and metric.
# MAGIC 5. **UC Model Registry** → `ward_<id>_champion` entries with `champion` alias, ready for serving.
