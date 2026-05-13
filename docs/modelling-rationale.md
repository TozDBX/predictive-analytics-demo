# Modelling rationale

## Why these four families

| Family | When it wins | Why it's in the bake-off |
|--------|--------------|--------------------------|
| **SeasonalNaive** | Never (we hope) | Honest baseline. If a model can't beat repeating last week, ship the baseline and save the compute. |
| **Prophet** | Strong weekly + annual seasonality, holiday effects, missing days | Easy to narrate to non-ML stakeholders, robust to outliers, well-understood by the SA / DE team. |
| **AutoARIMA (StatsForecast)** | Stationary series, short memory, no obvious seasonality dominance | Fast classical workhorse; the "what would a statistician fit by default" benchmark. |
| **AutoETS (StatsForecast)** | Additive seasonality + trend, smooth series | Often wins on quieter wards where noise is low and trend matters more than calendar effects. |

## Why batch (not streaming)

LBTH's current sensor estate exports CSVs once per day. Streaming would add
operational complexity without value until the sensor fleet moves to MQTT or
similar. The pattern is identical if/when streaming arrives — swap
`Trigger.AvailableNow` for `Trigger.ProcessingTime("1 minute")` and the
forecasting cadence becomes a separate concern.

## Why per-ward models, not a global model

Three reasons:

1. **Operational interpretability.** A ward councillor asking "why is my ward
   flagging?" deserves an answer about *that ward's* signal, not a global
   embedding.
2. **Failure isolation.** If Whitechapel's sensor goes offline, Bow East's
   forecast continues unaffected.
3. **Pattern fits the data.** 20 wards × 365 days is well-suited to local
   models. At 200+ time series a global / TFT-style model becomes attractive —
   `notebooks/04_collab_patterns.py` shows how to add TimesFM / Chronos when
   that day comes.

## When to swap models

- **Add TimesFM** when LBTH wants zero-shot forecasts on a *new* metric (NO2,
  O3) without retraining.
- **Add NeuralProphet** when external regressors (traffic counts, weather)
  start improving accuracy and we want richer interactions than Prophet's
  linear regressors.
- **Add Chronos / Moirai** when the team has 50+ wards and wants a single
  foundation model serving all of them.

The Many Model Forecasting framework supports all of these out of the box.

## Forecast horizon: 7 days

Chosen because:

- Matches the alert lead time LBTH residents would actually use (a week is
  long enough to plan, short enough that PM2.5 forecasts remain credible).
- Aligns with weekly seasonality — we forecast exactly one cycle ahead.
- Backtest stability — sMAPE on 7-day horizons is a well-understood
  benchmark in the air-quality literature.

A 3-day version is trivial to add as a second job parameter if Tower Hamlets'
public health team wants tighter alerting.
