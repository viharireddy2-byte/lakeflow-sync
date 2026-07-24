# lakeflow-sync

[![CI](https://github.com/your-username/lakeflow-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/lakeflow-sync/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Code Style](https://img.shields.io/badge/code%20style-ruff-000000)
![Type Checker](https://img.shields.io/badge/type%20checker-mypy-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A dual-mode ingestion pipeline that moves data from **PostgreSQL** into a **Databricks Lakehouse (Bronze layer)**, built on the [`dlt`](https://dlthub.com/) (Data Load Tool) library and orchestrated natively by **Databricks Lakeflow Jobs**.

It supports two operating modes:

- **Full Load** — snapshot-based initial/backfill load using `dlt`'s `sql_database` source.
- **CDC (Change Data Capture)** — near real-time streaming of `INSERT` / `UPDATE` / `DELETE` events off the Postgres write-ahead log (WAL), appended to Bronze as an **immutable event log** rather than a mirrored copy of the source.

> **Credit:** the dual-mode Full Load + CDC architecture and the "Bronze as an EL-only boundary" philosophy in this project were inspired by [victor-antoniassi/postgres-to-databricks-cdc](https://github.com/victor-antoniassi/postgres-to-databricks-cdc). All code here is an independent implementation, restructured, renamed, and extended with the enhancements described below.

---

## 🎯 Project Scope: Ingestion Only (EL, not ELT)

This project deliberately stops at **Extract & Load**. It does not attempt any business-logic transformation.

- **Role:** get data from the operational Postgres database into a queryable, versioned Bronze layer — reliably and cheaply.
- **Why stop here?** Coupling ingestion and transformation means a broken dbt model or a bad Spark job can take down data landing entirely. By keeping ingestion isolated, an upstream schema hiccup can be triaged without blocking new data from continuing to land, and Silver/Gold transformation (dbt, Spark SQL, whatever the team prefers) can evolve independently of how data gets in the door.

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion engine | Python 3.11+, [`dlt`](https://dlthub.com/) |
| Full Load source | `dlt.sources.sql_database` (standard SQL snapshot) |
| CDC source | `dlt.sources.pg_replication` (`pgoutput` logical replication), wrapped by this project's [`pg_stream`](src/lakeflow_sync/pg_stream) module to force append-only semantics, behind an import-safety shim (see [`source_compat`](src/lakeflow_sync/pg_stream/source_compat.py)) |
| Compute (quality gate) | PySpark |
| Destination | Databricks (Unity Catalog, Volumes, Delta Lake) |
| Source database | PostgreSQL (`wal_level=logical`) |
| Deployment | Databricks Asset Bundles (DABs), OAuth Service Principals |
| CI/CD | GitHub Actions |
| Quality engineering | `uv`, `ruff`, `mypy`, `pytest` + `pytest-cov` |

## 🚀 Features

- **Dual-mode operation** — `full_load` for initial/backfill snapshots, `cdc` for ongoing incremental capture.
- **Append-only Bronze log** — every CDC event (insert/update/delete) is appended as a new row tagged with `_cdc_operation`, `_cdc_lsn`, and `_cdc_committed_at`, so full history is preserved instead of being overwritten (see [Architecture](#-architecture)).
- **Databricks-native** — writes straight to Delta tables with schema evolution, staged through Unity Catalog Volumes.
- **Enterprise CI/CD** — OAuth Service Principal auth, isolated `dev_` / `qa_` / `prod_` catalogs, automated lint/type/test gates on every push and PR.
- **Structured JSON logging** — every log line is a single JSON object, so pipeline runs are directly searchable in Databricks Job run logs or any log aggregator. *(enhancement)*
- **Run-outcome notifications** — optional Slack-compatible webhook fires a summary (rows processed, duration, success/failure) at the end of every run. *(enhancement)*
- **Post-load data quality gate** — a lightweight PySpark-based check (`quality_checks.py`) runs as the final job task, asserting every Bronze table is non-empty and has no NULL primary keys before the job is marked successful. *(enhancement)*
- **`--dry-run` flag** — validate CLI args, catalog/dataset targeting, and configuration without writing any data. *(enhancement)*
- **Bounded retry with backoff, and no silent retries on the wrong failures** — transient errors (connection resets, lock timeouts) are retried automatically; permanent errors (bad credentials, missing tables) and a dropped/invalidated replication slot fail immediately with a clear, distinct exception instead of retrying forever or masking the problem. See [`resilience.py`](src/lakeflow_sync/resilience.py) and [Resilience & Retry Behavior](#-resilience--retry-behavior). *(enhancement)*
- **Real end-to-end CDC test** — an integration test (`tests/integration/`) runs `full_load` → mutates rows in a live Postgres replication slot → `cdc_load`, and asserts the append-only Bronze log is correct, rather than relying only on mocks. See [Integration Testing](#-integration-testing). *(enhancement)*

## ⏱️ Scheduling & Triggers

The bundled job trigger is set to **paused / manual** by default, intended for demoing on-request rather than burning idle compute in a portfolio/demo workspace.

For a real production deployment:

- **Hourly** — good default for batching up CDC events without excessive small-file writes to Delta.
- **Continuous** — switch the job trigger to continuous mode for near-real-time requirements; the CDC task will keep draining the replication slot as WAL activity arrives.

## 🏗️ Architecture

The pipeline runs in two mutually exclusive modes, each with a distinct write disposition:

1. **Full Load (`REPLACE`)** — takes a snapshot of configured source tables and replaces the Bronze table wholesale. Used for initialization and full resets.
2. **CDC (`APPEND`)** — continuously reads the WAL and appends every change event to the Bronze table.
   - **Update handling:** an update appends a *new* row carrying the new values, tagged `_cdc_operation="update"` — the prior version of the row is preserved, not overwritten.
   - **Delete handling:** a delete appends a marker row tagged `_cdc_operation="delete"` — a soft delete that preserves the fact that the row existed and was removed.

```mermaid
flowchart LR
    subgraph Source_System [Source System]
        PG[("PostgreSQL")]:::db_source
    end

    subgraph Lakeflow_Jobs [Databricks Lakeflow Jobs]
        subgraph Engine [Engine: dlt Library]
            direction TB
            FL[Mode: Full Load]:::white_box
            CDC[Mode: CDC Stream]:::white_box
        end

        subgraph Unity_Catalog [Unity Catalog Storage]
            subgraph Staging [Staging Layer]
                direction TB
                Vol_FL[Full Load Volume]:::white_box
                Vol_CDC[CDC Stream Volume]:::white_box
            end
            Delta[("Delta Table (Bronze)")]:::db_delta
        end

        QG[Quality Gate Task]:::white_box
    end

    PG -->|Snapshot Read| FL
    FL -->|"1. Write Parquet"| Vol_FL
    Vol_FL -.->|"2. REPLACE (swap)"| Delta

    PG -->|WAL Stream| CDC
    CDC -->|"1. Write Parquet"| Vol_CDC
    Vol_CDC ==>|"2. APPEND (history)"| Delta

    Delta --> QG

    classDef db_source fill:#212121,stroke:#000,color:#fff;
    classDef db_delta fill:#0277bd,stroke:#fff,color:#fff;
    classDef white_box fill:#ffffff,stroke:#90caf9,stroke-width:1px,color:#424242;
```

> **Terminology note:** "Full Load" here refers to the user-facing operation of replacing the destination dataset with the current source state (`write_disposition="replace"` in `dlt`). It is distinct from `dlt`'s internal use of "snapshot" state-tracking during logical replication initialization.

## 🔁 Resilience & Retry Behavior

Both `full_load` and `cdc_load` run their `dlt` pipeline call through [`resilience.run_with_retry`](src/lakeflow_sync/resilience.py), which classifies failures instead of treating every error the same way:

| Failure type | Example | Behavior |
|---|---|---|
| **Transient** | connection reset, lock timeout, momentary network blip | Retried up to 3 attempts total, with exponential backoff (2s, 4s, ...) |
| **Permanent** | bad credentials, missing table, permission denied | Raised immediately — retrying cannot fix these, so the job fails fast with a clear error |
| **Replication slot invalidated** | the CDC slot was dropped, or Postgres has already recycled the WAL segments it needed | Raised immediately as a dedicated `ReplicationSlotInvalidatedError` — retrying would either loop forever or silently skip changes. Recovery requires re-creating the slot/publication and running a fresh `full_load`, or increasing WAL retention (`wal_keep_size` / the slot's `max_slot_wal_keep_size`) |

A run that succeeds on its first attempt behaves identically to a direct `pipeline.run(...)` call — the retry wrapper only changes behavior on failure. Every retry, and the final classification, is logged (JSON) so it's visible in Databricks Job run logs.

## 📋 Prerequisites

- **Python 3.11+**
- **[uv](https://github.com/astral-sh/uv)** — fast Python package manager
- **Databricks workspace** with Unity Catalog enabled
- **PostgreSQL** with `wal_level=logical`

## ⚡ Quick Start (Local Execution)

### 1. Install dependencies

```bash
uv sync --all-extras
```

### 2. Configure secrets

Copy the example and fill in your credentials:

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

```toml
[sources.pg_replication.credentials]
database = "your_db"
password = "your_password"
host = "your_host"
port = 5432
username = "your_user"

[destination.databricks.credentials]
server_hostname = "dbc-xxxx.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/xxxx"
access_token = "dapi..."
```

> **Tip:** if the Databricks CLI is already configured locally, `dlt` can pick up your `DEFAULT` profile credentials automatically without needing them duplicated in `secrets.toml`.

### 3. Run Full Load (initialize)

```bash
uv run python -m lakeflow_sync.pipeline_main --mode full_load --catalog dev_orders_lakehouse --dataset bronze
```

### 4. Simulate transactions (optional)

Generate synthetic order activity in Postgres to exercise the CDC path:

```bash
# 5 inserts, 2 updates, 1 delete
export LAKEFLOW_SYNC_PG_DSN="postgresql://user:pass@host:5432/db"
uv run scripts/simulate_transactions.py 5 2 1
```

### 5. Run CDC Load (incremental)

```bash
uv run python -m lakeflow_sync.pipeline_main --mode cdc --catalog dev_orders_lakehouse --dataset bronze
```

### 6. Verify data

```bash
uv run scripts/verify_data.py --catalog dev_orders_lakehouse --dataset bronze
```

### 7. Run the data quality gate

```bash
uv run scripts/data_quality_check.py --catalog dev_orders_lakehouse --dataset bronze
```

## 🔄 CI/CD & Quality Engineering

Every push and pull request runs a strict validation pipeline via GitHub Actions (`.github/workflows/ci.yml`):

1. **Lint** — `uv run ruff check .`
2. **Type check** — `uv run mypy src/`
3. **Unit tests** — `uv run pytest` (coverage gate enforced via `pyproject.toml`)

### Environment strategy

Deployments are managed via **Databricks Asset Bundles**. Rather than provisioning separate workspaces per environment, this project uses a **logical isolation strategy**: a single workspace with distinct Unity Catalog catalogs per environment.

| Environment | Catalog | Trigger | Auth |
|---|---|---|---|
| Development | `dev_orders_lakehouse` | Local CLI | User credentials |
| QA | `qa_orders_lakehouse` | Push to `main` | Service Principal (OAuth M2M) |
| Production | `prod_orders_lakehouse` | GitHub Release | Service Principal (OAuth M2M) |

## 🧪 Integration Testing

`uv run pytest` (the default CI quality gate) only exercises unit-level logic with `dlt`'s pipeline and replication source mocked out — it does not prove the CDC path actually works against a live Postgres replication slot. `tests/integration/` closes that gap:

```bash
scripts/run_integration_tests.sh
```

This script:

1. Starts a local Postgres container with `wal_level=logical` via `docker-compose.yml`.
2. Runs `full_load` against a throwaway table, then mutates rows (insert / update / delete) and runs `cdc_load` against the **same live replication slot** used in production — pointed at a local `duckdb` file instead of a real Databricks workspace (`LAKEFLOW_SYNC_DESTINATION=duckdb`), so no Databricks credentials are required.
3. Asserts the resulting Bronze table is a correct append-only log: the update produces a *new* row rather than overwriting the original, and the delete lands as a soft-delete marker.
4. Tears the container down.

These tests are separate from the default `pytest` run on purpose (they need Docker and take longer); they self-skip if `LAKEFLOW_SYNC_PG_DSN` isn't set, so running plain `uv run pytest` on a machine without Docker is unaffected.

## ☁️ Deployment to Databricks

### 1. Provision catalogs

```bash
databricks catalogs create dev_orders_lakehouse
databricks catalogs create qa_orders_lakehouse
databricks catalogs create prod_orders_lakehouse
```

### 2. Set up secrets in Databricks

```bash
databricks secrets create-scope lakeflow_sync_scope
databricks secrets put-secret lakeflow_sync_scope pg_connection_string --string-value "postgresql://user:pass@host:port/db"
```

### 3. Deploy the bundle (manual / dev)

```bash
databricks bundle deploy -t dev --profile DEFAULT
```

### 4. Run jobs

```bash
# Full load task
databricks bundle run lakeflow_sync_job --task-key full_load_task --profile DEFAULT

# CDC task
databricks bundle run lakeflow_sync_job --task-key cdc_load_task --profile DEFAULT

# Quality gate task
databricks bundle run lakeflow_sync_job --task-key data_quality_task --profile DEFAULT
```

CI/CD (`.github/workflows/deploy.yml`) automates this: a push to `main` deploys to QA using a Service Principal; publishing a GitHub Release deploys to Production.

## 📊 Data Model

Source tables ingested by default (configurable via `LAKEFLOW_SYNC_TABLES`):

| Table | Primary key | Notes |
|---|---|---|
| `customers` | `customer_id` | Reference/dimension-like data |
| `orders` | `order_id` | High-change-rate transactional table — primary CDC target |
| `order_items` | `order_item_id` | Line items per order |
| `products` | `product_id` | Reference/dimension-like data |

Every table in Bronze carries these CDC metadata columns in addition to the source columns:

| Column | Description |
|---|---|
| `_cdc_operation` | `insert`, `update`, or `delete` |
| `_cdc_lsn` | Postgres WAL log sequence number — useful for ordering/dedup downstream |
| `_cdc_committed_at` | UTC timestamp the change was committed on the source database |

## 📂 Project Structure

```
.
├── pyproject.toml                 # Project definition, dependencies, tool configs
├── databricks.yml                 # Databricks Asset Bundle (DABs) definition
├── docker-compose.yml             # Local Postgres (wal_level=logical) for integration tests
├── .github/workflows/             # CI (lint/type/test) and CD (bundle deploy) pipelines
├── src/
│   └── lakeflow_sync/
│       ├── __init__.py
│       ├── pipeline_main.py       # CLI / job entry point
│       ├── full_load.py           # Full Load pipeline logic
│       ├── cdc_load.py            # CDC incremental pipeline logic
│       ├── quality_checks.py      # Post-load data quality gate
│       ├── resilience.py          # Retry/backoff + failure classification for pipeline runs
│       ├── utils/                 # Logging, notifications
│       └── pg_stream/             # Custom append-only CDC event normalization
│           └── source_compat.py   # Import-safety shim for dlt.sources.pg_replication
├── tests/                         # Unit tests (mocked -- no external services required)
│   └── integration/               # End-to-end CDC test against real Postgres logical replication
├── scripts/                       # Local helper tools (simulate, verify, quality check,
│                                   # run_integration_tests.sh)
├── resources/jobs/                # Databricks Lakeflow Job resource definitions
└── .dlt/                          # Local dlt config / secrets (gitignored)
```

## ⚠️ Known Limitations

- **Serverless network egress:** on constrained free-tier Databricks Serverless compute, connections to Unity Catalog Volumes storage endpoints can be blocked by egress restrictions, surfacing as `Connection refused`. Workaround: run locally (Quick Start above) or on classic (non-serverless) compute.
- **Single replication slot:** the CDC task assumes one logical replication slot per environment. Running multiple concurrent CDC jobs against the same slot will cause contention — scale by adding slots/publications per table group if needed.
- **`dlt.sources.pg_replication` is a verified source, not a guaranteed stable import.** dlt historically ships this as a "verified source" meant to be vendored via `dlt init pg_replication databricks` rather than imported directly the way `sql_database` is. This project imports it directly (behind [`source_compat.get_replication_source()`](src/lakeflow_sync/pg_stream/source_compat.py), which fails with a clear, actionable error rather than a bare `ModuleNotFoundError` if a given `dlt` version doesn't expose it) — but if you hit that error, vendoring the source per the shim's docstring is the fix, and it hasn't been re-verified against every `dlt` release.
- **Retry policy is message-based, not error-code-based.** `resilience.classify_error` classifies failures by matching substrings in the exception's string representation (see [Resilience & Retry Behavior](#-resilience--retry-behavior)). This is deliberately conservative — unrecognized errors default to "transient" and get retried rather than failing instantly — but it means a Postgres/dlt version that changes its error wording could be misclassified until the marker lists are updated.
- **Integration test coverage is CDC-happy-path only.** `tests/integration/test_cdc_end_to_end.py` proves one insert/update/delete cycle round-trips correctly through a live replication slot; it does not yet cover concurrent writers, large batches, schema changes mid-stream, or an actual slot invalidation (dropped slot / WAL already recycled) end-to-end — those failure modes are covered at the unit level (`tests/test_cdc_load.py`, `tests/test_resilience.py`) with mocks, not against real Postgres.

## Ideas for Further Enhancement

- Swap the webhook notifier for a native Databricks SQL Alert on the quality-gate table.
- Add schema-drift detection that fails fast (rather than silently evolving) for a configurable list of "protected" columns.
- Partition the CDC append table by `_cdc_committed_at` date for cheaper time-bound queries at scale.

## License

MIT — see [LICENSE](LICENSE).
