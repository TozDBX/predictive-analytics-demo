# Reference architecture

## End-to-end: batch ingest → forecast → SMS alert

```mermaid
flowchart LR
    subgraph SRC["Sources (LBTH)"]
        S1[ADLS Gen2<br/>sensor exports<br/>daily CSV]
        S2[(Resident<br/>register)]
    end

    subgraph DBX["Azure Databricks · fevm-mbcl"]
        direction TB
        AL[Auto Loader<br/>Trigger.AvailableNow]
        subgraph SDP["Lakeflow Spark Declarative Pipeline"]
            B[Bronze<br/>raw_readings]
            SI[Silver<br/>readings_typed<br/>+ DQ expectations]
            G[Gold<br/>ward_daily_ts<br/>+ residents<br/>+ subscriptions]
        end
        subgraph ML["Multi-model bake-off"]
            M1[SeasonalNaive]
            M2[Prophet]
            M3[StatsForecast<br/>AutoARIMA]
            M4[StatsForecast<br/>AutoETS]
        end
        MLF[(MLflow<br/>shared experiment)]
        REG[(UC Model Registry<br/>ward_champion)]
        SC[Score job<br/>champion-per-ward]
        AL --> B --> SI --> G
        G --> M1 & M2 & M3 & M4
        M1 & M2 & M3 & M4 --> MLF
        MLF --> REG
        REG --> SC
        G --> SC
        SC --> AT[(gold_alerts)]
    end

    subgraph EGRESS["Alert egress (Azure)"]
        WH[Webhook<br/>HTTPS + basic auth]
        AF[Azure Function<br/>signs + dedupes]
        ACS[Azure Communication<br/>Services SMS]
    end

    R[(Resident<br/>mobile)]

    S1 -. daily upload .-> AL
    S2 -. nightly extract .-> G
    AT --> WH --> AF --> ACS --> R

    classDef gov fill:#fde68a,stroke:#92400e,color:#92400e;
    UC[Unity Catalog<br/>governance · lineage<br/>tagging · audit]:::gov
    UC -.- SDP
    UC -.- ML
    UC -.- REG
```

## What governs what

| Concern | Where it's enforced |
|---------|---------------------|
| Access to sensor + resident data | UC table grants on `mbcl_catalog.th_air_quality.*` and `*.residents`; row filter on `subscriptions` (only opted-in) |
| Phone number column | UC column mask (full mask for analysts, plain text only for the alert service principal) |
| Cost attribution | Compute tags `team`, `project`, `env`; surfaced in the existing cost dashboard |
| Model promotion | UC Model Registry alias `champion@ward_<id>` set only via Asset Bundle on `main` |
| Alert audit trail | Every webhook payload + ACS response written to `gold_alerts_audit` (append-only Delta) |
| SMS opt-in | `subscriptions.opt_in_status = 'ACTIVE'` filter is the **only** path that produces an alert payload |

## Why an Azure Function fronts ACS

Databricks webhooks send HTTPS POSTs with basic auth, but ACS expects a managed-identity-authenticated call against the Communication Services REST API. The Function:

1. Authenticates to ACS using its own managed identity (no secrets in Databricks).
2. Re-signs / shapes the payload.
3. Deduplicates by `alert_key` (ward + DAQI band + day) so a re-run never doubles up.
4. Writes the ACS message ID + delivery status back to `gold_alerts_audit` via a small Lakebase or Delta sink.

This keeps the SMS credential out of the data platform and gives privacy a clean integration boundary.

## Failure / retry path

```mermaid
sequenceDiagram
    participant Job as Daily Job
    participant Alerts as gold_alerts
    participant WH as Webhook
    participant Fn as Azure Function
    participant ACS as ACS SMS
    participant Audit as gold_alerts_audit

    Job->>Alerts: write new alerts
    Job->>WH: POST batch payload
    WH->>Fn: HTTPS + basic auth
    Fn->>Fn: dedupe by alert_key
    Fn->>ACS: send SMS (managed identity)
    alt ACS success
        ACS-->>Fn: message_id, accepted
        Fn->>Audit: status=DELIVERED
    else ACS failure
        ACS-->>Fn: error
        Fn->>Audit: status=FAILED + reason
        Fn->>Fn: retry x3 (exponential backoff)
    end
```

If the webhook itself fails, Databricks job notifications retry — and the next day's run is idempotent (`alert_key` already in audit → skipped).
