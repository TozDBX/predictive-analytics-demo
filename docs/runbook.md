# Operational runbook

## Daily expectations

| Time (Europe/London) | What happens |
|----------------------|--------------|
| 06:00 | Job `th_air_quality_daily` triggers |
| 06:00–06:05 | Lakeflow pipeline ingests overnight CSVs (bronze → silver → gold) |
| 06:05–06:20 | `train_and_compare` runs the 4-model bake-off, updates champions |
| 06:20–06:25 | `score_and_alert` produces forecasts, writes `gold_alerts`, fans out webhook |
| 06:30 | First SMS deliveries land via ACS |

## Alert rules

- Only `daqi.band >= 4` (Moderate or worse) generate alerts.
- A subscription's `daqi_threshold` further filters: residents who set
  threshold = 6 only get alerted on High+, etc.
- One alert per `(subscription_id, ward_id, forecast_date, daqi_band)` —
  enforced via `alert_key` SHA256 + idempotent MERGE.

## On-call playbook

| Symptom | First check | Likely cause |
|---------|-------------|--------------|
| Pipeline failing on `bronze_readings` | Auto Loader schema location, source file presence in `/Volumes/.../raw_csv/` | Late or missing daily upload |
| `train_and_compare` task slow | Cluster size, shared experiment is reachable | Backtest window too wide; reduce `BACKTEST_DAYS` parameter |
| `score_and_alert` succeeds but no SMS arrives | Webhook secret, ACS Function logs | Webhook URL stale, or ACS quota exceeded |
| Duplicate SMS to same resident | `gold_alerts` MERGE sanity, audit table | Webhook target not honouring `alert_key` for dedupe |

## Toggle alert sending

```bash
# Pause alerting (still write gold_alerts, no webhook fan-out)
databricks bundle run th_air_quality_daily -t prod --var alert_dry_run=true

# Resume
databricks bundle run th_air_quality_daily -t prod --var alert_dry_run=false
```

## Champion freeze (for an event)

When a public event needs deterministic forecasts (e.g. London Marathon
weekend), copy the champions table into a frozen variant:

```sql
CREATE OR REPLACE TABLE lbth_data_platform.air_quality_forecast.gold_ward_champion_frozen
AS SELECT * FROM lbth_data_platform.air_quality_forecast.gold_ward_champion;
```

Switch the scoring notebook to read from `*_frozen` for the duration, then
revert. The bundle exposes a `champion_table` variable for this.

## Quarterly hygiene

- Archive MLflow runs older than 6 months out of the shared experiment.
- Re-evaluate the model roster — drop any family that hasn't been a champion
  on any ward for two consecutive quarters.
- Refresh DAQI band thresholds against DEFRA's published tables (they update
  periodically).
- Audit `gold_active_subscriptions` against the resident register — confirm
  `REVOKED` records actually drop out of alerting.
