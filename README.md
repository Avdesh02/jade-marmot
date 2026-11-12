# jade-marmot — wind turbine telemetry on Databricks

A Spark Declarative Pipeline (SDP) for wind-farm SCADA telemetry, parameterised per site,
deployed as a Declarative Automation Bundle (DAB) with CI/CD, unit tests, and a test-data
simulation pipeline.

The original prototype scripts are kept, unchanged, in `wind_turbine_project/`. The bundle
doesn't deploy them.

## Naming standard — `{{model_name}}`

Every Databricks object carries the git folder name, normalised to a UC identifier
(`jade-marmot` → `jade_marmot`). `scripts/model_name.py` derives it and CI exports it as
`BUNDLE_VAR_model_name`. A unit test fails if the bundle default drifts from the folder name.

| Object | Pattern | dev example |
|---|---|---|
| Catalog | `{model_name}_{target}` | `jade_marmot_dev` |
| Pipeline / jobs | `{model_name}_{target}_{purpose}` | `jade_marmot_dev_wind_telemetry_sdp` |
| Workspace folder | `/Workspace/Users/<you>/{model_name}/{target}` | `.../jade_marmot/dev` |
| Tables | `{catalog}.{layer}.{table}` | `jade_marmot_dev.gold.site_daily_kpis` |
| Tags | `model_name: {model_name}` on every job and the pipeline | |

## Pipeline (`resources/wind_telemetry.pipeline.yml`, `src/pipeline/transformations/`)

```
landing Volume                      bronze                    silver                              gold
sensors/<site>/*.json  ──append flow per site──▶ sensor_readings_raw ─▶ sensor_readings (dedupe) ─▶ turbine_alerts (ST)
                                                               └▶ sensor_readings_quarantine    turbine_hourly_metrics (MV, SCD2 as-of)
turbine_cdc/<site>/*.json ─append flow per site─▶ turbine_cdc_raw ─AUTO CDC─▶ dim_turbine (SCD2)  site_daily_kpis (MV)
                                                                  └AUTO CDC─▶ turbine_current (SCD1) fleet_status (MV)
config/sites.json ─────────────────────────────────────────────▶ dim_site (MV)
```

* **Append flows**: `dp.append_flow`, one Auto Loader flow per enabled site into a shared
  streaming table. Each flow keeps its own checkpoint, so enabling a site adds a flow
  without a full refresh.
* **Change data flow**: `dp.create_auto_cdc_flow` maintains SCD Type 2 (`dim_turbine`) and
  SCD Type 1 (`turbine_current`) from the same change feed. Change Data Feed is also
  enabled on bronze and silver readings.
* **Streaming flows**: bronze → silver → `turbine_alerts`, with rule-based quarantine
  and watermark de-duplication.
* **Materialized views**: `dim_site`, `turbine_hourly_metrics`, `site_daily_kpis`,
  `fleet_status`.

The logic lives in `src/jade_marmot_wind/transforms.py` as plain PySpark functions.
Transformation files only wire them to SDP decorators, which is what makes them
unit-testable.

## Parameters

**Static** parameters are fixed per target at deploy time (bundle variables → pipeline
`configuration` / task args):

| Variable | dev | prod |
|---|---|---|
| `enabled_sites` | `RJ01,TN01,GJ01` | `all` |
| `simulation_concurrency` | 3 | 5 |
| `schedule_pause_status` | PAUSED | UNPAUSED |
| `dedupe_watermark`, `gearbox_oil_temp_*`, `landing_root`, `catalog` | per target | |

**Dynamic** parameters are chosen per run (job parameters and runtime references):

| Parameter | Effect |
|---|---|
| `site_ids` | `all` or a subset of the enabled sites; validated against `enabled_sites` |
| `scenario` | `normal`, `storm`, `heatwave`, `maintenance`, `repower` |
| `minutes`, `fault_rate`, `start_ts`, `seed`, … | simulation shape |
| `full_refresh` | `condition_task` picks the full or incremental pipeline branch |
| `{{job.run_id}}`, `{{tasks.plan_sites.values.sites}}`, `{{input.site_id}}` | run id, task values, for_each input |

## Jobs

* **`…_wind_test_data_simulator`**: `plan_sites` (static × dynamic site selection → task
  value), then a `for_each` with one `simulate_site` task per site. It writes JSON-lines
  telemetry and CDC files plus a manifest of the expected outcome.
* **`…_wind_telemetry_orchestrator`**: `run_job_task` (simulator) → `condition_task`
  (full_refresh?) → `pipeline_task` (full **or** incremental) → `validate_outputs`
  (`run_if: AT_LEAST_ONE_SUCCESS`) → `publish_alert_digest`.
* **`…_wind_unit_tests`**: pytest on serverless against the deployed files.

Every job runs on serverless and uses the bundle-built wheel (`jade_marmot_wind`).

## Simulator

`src/jade_marmot_wind/simulator.py` is pure Python and deterministic. The seed is derived
from the site, the window and the scenario, so a rerun reproduces the same `reading_id`s
and silver de-duplicates them. It models an idealised power curve, diurnal wind, an AR(1)
gust process, and scenario effects: storm cut-out, heatwave temperatures, maintenance and
repower CDC events. It also injects faults (nulls, negative power, bad timestamps,
temperature spikes, site mismatches, duplicates), and the validation job checks that each
fault was quarantined.

## Running it

```bash
pip install -r requirements-dev.txt
pytest tests/unit                                   # local Spark (Java 8/17)

export BUNDLE_VAR_model_name=$(python scripts/model_name.py)
python scripts/bootstrap_catalog.py --target dev --profile wind
databricks bundle deploy -t dev -p wind
databricks bundle run -t dev -p wind wind_unit_tests
databricks bundle run -t dev -p wind wind_telemetry_orchestrator \
  --params site_ids=RJ01,scenario=storm,minutes=60,full_refresh=false
```

**Why the catalog is bootstrapped outside the bundle:** this workspace uses Default Storage,
and the UC catalogs REST API the bundle calls rejects it ("Please use the UI to create a
catalog with Default Storage"). SQL `CREATE CATALOG` works, so
`scripts/bootstrap_catalog.py` runs it idempotently before each deploy. Schemas, the
volume, the pipeline and the jobs are all bundle-managed.

## CI/CD (`.github/workflows/ci-cd.yml`)

| Trigger | Steps |
|---|---|
| Pull request | ruff, local pytest, `bundle validate` for dev and prod |
| Push to `main` | + bootstrap catalog, deploy dev, unit tests on serverless, end-to-end orchestrator run |
| Tag `v*` | deploy prod (gate it with required reviewers on the `prod` environment) |

Repository secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`,
`DATABRICKS_CLIENT_SECRET` for the `jade_marmot_ci` service principal (OAuth M2M, scope
`all-apis`). The service principal needs `ALL PRIVILEGES` and `MANAGE` on the target
catalog, plus `CREATE CATALOG` for the first prod deploy (it gets that from `admins`).
