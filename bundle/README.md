# Bundle deploy & collaboration

## Prereqs

- Databricks CLI ≥ 0.235
- Azure CLI logged in (`az login`) for the target workspace
- A Databricks profile pointing at the workspace (`fevm-mbcl` for dev)

## Commands

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy pipeline + job to dev (default target)
databricks bundle deploy -t dev

# Trigger a one-shot run for the demo
databricks bundle run th_air_quality_daily -t dev

# Promote to prod (requires deployer SP)
databricks bundle deploy -t prod
```

## What the bundle creates

| Resource | Notes |
|----------|-------|
| Lakeflow Declarative Pipeline `th_air_quality_forecast_pipeline` | Serverless, photon, batch |
| Job `th_air_quality_daily` | 06:00 Europe/London, paused by default |
| MLflow experiment | `/Shared/lbth/air_quality_forecasting/experiments/multi_model_bakeoff` — kept in `/Shared`, not a Git folder, because workspace MLflow experiments cannot live inside Git folders |
| Notebook tasks | Reference notebooks via relative paths from `bundle/` |

## Webhook secret (when leaving DRY_RUN)

```bash
databricks secrets create-scope lbth-air-quality
databricks secrets put-secret lbth-air-quality webhook_url --string-value "https://<your-azure-function>.azurewebsites.net/api/lbth-alerts"
```

Then redeploy with `--var alert_dry_run=false` or set in the prod target.

## Permissions checklist (run once on the LBTH workspace)

- Add `lbth-data-platform-deployers` to **Manage** on the bundle root.
- Grant `USE CATALOG` on `lbth_data_platform` to the data team.
- Grant `SELECT` on `gold_*` tables to BI/analyst groups (read-only).
- Grant `SELECT` on `phone_e164` (raw) **only** to the alert SP — analysts see masked.
- Tag the deployer SP and the alert SP under your governed-tag policy `data-classification = restricted-personal`.
