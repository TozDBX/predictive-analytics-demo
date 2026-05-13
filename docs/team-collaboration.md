# Team collaboration model

How the LBTH data team would run this as an internal project. The pattern
follows the Databricks-recommended **Git folders + shared workspace experiment**
guidance — see [CI/CD with Git folders](https://learn.microsoft.com/en-us/azure/databricks/repos/ci-cd)
and [Organize training runs with MLflow experiments](https://learn.microsoft.com/en-us/azure/databricks/mlflow/experiments).

## Repo layout (Azure DevOps or GitHub)

```
lbth-data-platform/
└── air-quality-forecasting/   ← this folder, mirroring demo layout
    ├── notebooks/
    ├── pipeline/
    ├── jobs/
    ├── bundle/
    └── docs/
```

## Branch model

| Branch | Purpose | Protection |
|--------|---------|-----------|
| `main` | Reflects what is deployed to **prod** | Protected: PR review + CI green required |
| `develop` | Integration branch; auto-deploys to **dev** | Protected: PR review required |
| `feature/<ticket>-<slug>` | Engineer-owned feature branches | Free |

PR flow: `feature/*` → `develop` → `main`. The Asset Bundle uses `bundle.target`
to switch catalog, host, and `dry_run` between dev and prod.

## Per-engineer workspace setup

```text
/Workspace/
└── Users/<your.email>/
    └── air-quality-forecasting/         ← Git folder, your own clone
        └── (checkout of feature/<ticket> or develop)
```

In the workspace UI: **Workspace → Users → your.email → New → Git folder**.
Point at the repo, pick your branch, set the path under your user folder.

## Shared MLflow experiment

```text
/Shared/lbth/air_quality_forecasting/experiments/
└── multi_model_bakeoff   ← every engineer's runs land here
```

**Why `/Shared` and not the Git folder?** Workspace MLflow experiments cannot
be created inside Git folders. The notebook code points at the `/Shared`
path explicitly so any engineer's runs end up in the same experiment
regardless of which Git folder they run from.

## Run tagging convention

Every MLflow run from `02_train_and_compare` gets these tags:

| Tag | Source | Purpose |
|-----|--------|---------|
| `model` | Hardcoded per family | Filter by Prophet, AutoARIMA, etc. |
| `ward_id` | Per-ward loop | Compare runs across the same ward |
| `run_owner` | `notebook context userName` | Filter to your own runs |
| `branch` | `GIT_BRANCH` env (set by the bundle) | Separate experimental branches from main |
| `git_commit` | `GIT_COMMIT` env (set by the bundle) | Reproduce a specific run |
| `run_purpose` | `bakeoff` for orchestrated runs, free-text for ad-hoc | Distinguish prod from manual exploration |

These tags drive the filter recipes in `notebooks/04_collab_patterns.py`.

## CODEOWNERS (suggested)

```text
# Notebooks — ML lead
notebooks/02_train_and_compare.py    @lbth/ml-lead
notebooks/03_score_and_alert.py      @lbth/ml-lead @lbth/privacy

# Pipeline — DE lead
pipeline/                            @lbth/data-engineering-lead

# Bundle / promotion — both
bundle/                              @lbth/ml-lead @lbth/data-engineering-lead

# Architecture — both + privacy on alerting
architecture/                        @lbth/ml-lead @lbth/data-engineering-lead @lbth/privacy
```

## Cost attribution

The job carries cluster/job tags `team=data-platform`, `project=air-quality-forecasting`,
`env=dev|prod`. Combined with the [governed tag policy](https://learn.microsoft.com/en-us/azure/databricks/admin/governed-tags/),
this attributes spend cleanly in the cost dashboard already shared with the team.

## CI (Azure Pipelines example)

```yaml
trigger:
  branches:
    include: [develop, main]

stages:
  - stage: validate
    jobs:
      - job: bundle_validate
        steps:
          - script: |
              curl -sSL https://databricks.com/install.sh | sh
              databricks bundle validate -t $TARGET
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
              TARGET: $[ replace(variables['Build.SourceBranchName'], 'main', 'prod') ]

  - stage: deploy
    dependsOn: validate
    condition: succeeded()
    jobs:
      - deployment: deploy_bundle
        environment: databricks-${{ variables.TARGET }}
        strategy:
          runOnce:
            deploy:
              steps:
                - script: databricks bundle deploy -t $TARGET
```
