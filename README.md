# lakeflow-sync

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Code Style](https://img.shields.io/badge/code%20style-ruff-000000)
![Type Checker](https://img.shields.io/badge/type%20checker-mypy-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A dual-mode ingestion pipeline for landing PostgreSQL data into a Databricks Lakehouse Bronze layer: a `full_load` mode for initial/backfill snapshots, and a `cdc` mode that streams `INSERT`/`UPDATE`/`DELETE` events off the Postgres write-ahead log and appends them as an immutable event log — no row is ever overwritten in place.

Built on [`dlt`](https://dlthub.com/) for extraction/loading, orchestrated as a Databricks Lakeflow Job, deployed via Databricks Asset Bundles.

**Note on lineage:** the overall shape of this project — dlt-based EL split into full/CDC modes, append-only Bronze with soft deletes, DAB-based dev/qa/prod deployment — follows a pattern common to several public Postgres-to-Databricks CDC reference implementations. What's below is my own implementation of that pattern, built out with the retry/resilience layer, data quality gate, structured logging, notifications, and integration test suite described under [What This Adds Beyond the Basic Pattern](#what-this-adds-beyond-the-basic-pattern).

## Why ingestion-only

This project stops at Extract & Load on purpose — it does not transform data. The reasoning: a broken dbt model or a bad Spark job shouldn't be able to stop new data from landing. Keeping ingestion isolated means an upstream schema hiccup can be triaged on its own timeline, and Silver/Gold transformation logic (dbt, Spark SQL, whatever the team prefers) evolves independently of how data gets in the door.

## How it works

The pipeline runs in two mutually exclusive modes, each with a distinct write disposition:

- **`full_load` (`REPLACE`)** — snapshots the configured source tables via `dlt`'s `sql_database` source and replaces the Bronze table wholesale. Used for initialization and full resets.
- **`cdc` (`APPEND`)** — reads the Postgres WAL via logical replication (`pgoutput`) and appends every change event to Bronze:
  - an **update** appends a new row carrying the new values, tagged `_cdc_operation="update"` — the prior version is preserved, not overwritten.
  - a **delete** appends a marker row tagged `_cdc_operation="delete"` — a soft delete that records the row existed and was removed, rather than erasing it.

```mermaid
flowchart TD
    PG[("PostgreSQL")]:::db_source

    subgraph modes [dlt pipeline]
        FL["full_load\n(sql_database source)"]:::box
        CDC["cdc_load\n(pg_stream / WAL)"]:::box
    end

    Vol[("Unity Catalog Volume\nstaging")]:::box
    Bronze[("Delta Bronze table")]:::db_delta
    QG["quality_checks.py"]:::box

    PG -->|snapshot| FL
    PG -->|WAL stream| CDC
    FL --> Vol
    CDC --> Vol
    Vol -->|REPLACE| Bronze
    Vol ==>|APPEND| Bronze
    Bronze --> QG

    classDef db_source fill:#212121,stroke:#000,color:#fff;
    classDef db_delta fill:#0277bd,stroke:#fff,color:#fff;
    classDef box fill:#ffffff,stroke:#90caf9,stroke-width:1px,color:#424242;
```

> **Terminology:** "Full Load" here is the user-facing operation of replacing the destination with the current source state (`write_disposition="replace"` in `dlt`) — distinct from `dlt`'s internal "snapshot" state-tracking during logical replication initialization.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion engine | Python 3.11+, [`dlt`](https://dlthub.com/) |
| Full Load source | `dlt.sources.sql_database` |
| CDC source | `dlt.sources.pg_replication` (`pgoutput`), wrapped by [`pg_stream`](src/lakeflow_sync/pg_stream) to force append-only semantics, behind the [`source_compat`](src/lakeflow_sync/pg_stream/source_compat.py) import shim |
| Compute (quality gate) | PySpark |
| Destination | Databricks (Unity Catalog, Volumes, Delta Lake) |
| Source database | PostgreSQL (`wal_level=logical`) |
| Deployment | Databricks Asset Bundles, OAuth Service Principals |
| CI/CD | GitHub Actions |
| Quality engineering | `uv`, `ruff`, `mypy`, `pytest` + `pytest-cov` |

## What this adds beyond the basic pattern

- **Retry with failure classification, not blanket retry** — transient errors (connection resets, lock timeouts) get retried with backoff; permanent errors (bad credentials, missing tables) and an invalidated replication slot fail immediately with a distinct exception instead of looping forever or masking the problem. See [Resilience](#resilience--retry-behavior).
- **Post-load data quality gate** — `quality_checks.py` runs as the final job task and asserts every Bronze table is non-empty with no NULL primary keys before the job is marked successful.
- **Structured JSON logging** — every log line is a single JSON object, searchable directly in Databricks Job run logs or any aggregator.
- **Run-outcome notifications** — an optional webhook posts a summary (rows processed, duration, success/failure) at the end of every run.
- **`--dry-run` flag** — validates CLI args and configuration without writing any data.
- **A real end-to-end CDC test**, not just mocks — `tests/integration/` runs `full_load` → mutates rows against a live Postgres replication slot → `cdc_load`, and asserts the resulting append-only log is correct. See [Integration Testing](#integration-testing).

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- A Databricks workspace with Unity Catalog enabled
- PostgreSQL with `wal_level=logical`

## Quick start (local)

```bash
# 1. Install dependencies
uv sync --all-extras

# 2. Configure secrets
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

> If the Databricks CLI is already configured locally, `dlt` can pick up your `DEFAULT` profile credentials without duplicating them in `secrets.toml`.

```bash
# 3. Initialize with a full load
uv run python -m lakeflow_sync.pipeline_main --mode full_load --catalog dev_orders_lakehouse --dataset bronze

# 4. (optional) generate synthetic activity to exercise CDC
export LAKEFLOW_SYNC_PG_DSN="postgresql://user:pass@host:5432/db"
uv run scripts/simulate_transactions.py 5 2 1   # 5 inserts, 2 updates, 1 delete

# 5. Run CDC load
uv run python -m lakeflow_sync.pipeline_main --mode cdc --catalog dev_orders_lakehouse --dataset bronze

# 6. Verify
uv run scripts/verify_data.py --catalog dev_orders_lakehouse --dataset bronze

# 7. Run the quality gate
uv run scripts/data_quality_check.py --catalog dev_orders_lakehouse --dataset bronze
```

## Resilience & retry behavior

Both `full_load` and `cdc_load` run their `dlt` pipeline call through [`resilience.run_with_retry`](src/lakeflow_sync/resilience.py), which classifies failures rather than treating every error identically:

| Failure type | Example | Behavior |
|---|---|---|
| Transient | connection reset, lock timeout, momentary network blip | Retried up to 3 attempts, exponential backoff (2s, 4s, ...) |
| Permanent | bad credentials, missing table, permission denied | Raised immediately — retrying can't fix these |
| Replication slot invalidated | slot was dropped, or Postgres already recycled the needed WAL segments | Raised immediately as `ReplicationSlotInvalidatedError`. Recovery requires re-creating the slot/publication and running a fresh `full_load`, or increasing WAL retention (`wal_keep_size` / the slot's `max_slot_wal_keep_size`) |

A run that succeeds on the first attempt behaves identically to a direct `pipeline.run(...)` call — the wrapper only changes behavior on failure. Every retry and the final classification are logged in JSON, so they're visible in Databricks Job run logs.

## CI/CD

Every push and pull request runs a validation pipeline (`.github/workflows/ci.yml`):

1. Lint — `uv run ruff check .`
2. Type check — `uv run mypy src/`
3. Unit tests — `uv run pytest` (coverage gate enforced via `pyproject.toml`)

### Environment strategy

Deployments go through Databricks Asset Bundles. Rather than separate workspaces per environment, this uses logical isolation: one workspace, distinct Unity Catalog catalogs per environment.

| Environment | Catalog | Trigger | Auth |
|---|---|---|---|
| Development | `dev_orders_lakehouse` | Local CLI | User credentials |
| QA | `qa_orders_lakehouse` | Push to `main` | Service Principal (OAuth M2M) |
| Production | `prod_orders_lakehouse` | GitHub Release | Service Principal (OAuth M2M) |

## Integration testing

The default CI run (`uv run pytest`) only exercises unit-level logic with `dlt`'s pipeline and replication source mocked out — it doesn't prove the CDC path works against a live replication slot. `tests/integration/` closes that gap:

```bash
scripts/run_integration_tests.sh
```

This script:

1. Starts a local Postgres container with `wal_level=logical` via `docker-compose.yml`.
2. Runs `full_load` against a throwaway table, mutates rows (insert/update/delete), then runs `cdc_load` against that **same live replication slot** — pointed at a local `duckdb` file instead of a real Databricks workspace (`LAKEFLOW_SYNC_DESTINATION=duckdb`), so no Databricks credentials are needed.
3. Asserts the resulting Bronze table is a correct append-only log: updates produce new rows rather than overwriting, deletes land as soft-delete markers.
4. Tears the container down.

These tests are separate from the default `pytest` run on purpose — they need Docker and take longer — and self-skip if `LAKEFLOW_SYNC_PG_DSN` isn't set, so plain `uv run pytest` on a machine without Docker is unaffected.

## Deploying to Databricks

```bash
# 1. Provision catalogs
databricks catalogs create dev_orders_lakehouse
databricks catalogs create qa_orders_lakehouse
databricks catalogs create prod_orders_lakehouse

# 2. Set up secrets
databricks secrets create-scope lakeflow_sync_scope
databricks secrets put-secret lakeflow_sync_scope pg_connection_string --string-value "postgresql://user:pass@host:port/db"

# 3. Deploy the bundle (dev)
databricks bundle deploy -t dev --profile DEFAULT

# 4. Run job tasks
databricks bundle run lakeflow_sync_job --task-key full_load_task --profile DEFAULT
databricks bundle run lakeflow_sync_job --task-key cdc_load_task --profile DEFAULT
databricks bundle run lakeflow_sync_job --task-key data_quality_task --profile DEFAULT
```

`.github/workflows/deploy.yml` automates the rest: a push to `main` deploys to QA under a Service Principal; publishing a GitHub Release deploys to Production.

The job trigger ships **paused/manual** by default — this is meant to be demoed on request rather than burn idle compute in a portfolio workspace. For a real deployment, **hourly** is a reasonable default for batching CDC events without excessive small-file writes to Delta; switch to **continuous** if near-real-time matters more than file-size efficiency.

## Data model

Source tables ingested by default (configurable via `LAKEFLOW_SYNC_TABLES`):

| Table | Primary key | Notes |
|---|---|---|
| `customers` | `customer_id` | Reference/dimension-like |
| `orders` | `order_id` | High-change-rate — primary CDC target |
| `order_items` | `order_item_id` | Line items per order |
| `products` | `product_id` | Reference/dimension-like |

Every Bronze table carries these CDC metadata columns alongside the source columns:

| Column | Description |
|---|---|
| `_cdc_operation` | `insert`, `update`, or `delete` |
| `_cdc_lsn` | Postgres WAL log sequence number — useful for ordering/dedup downstream |
| `_cdc_committed_at` | UTC timestamp the change was committed on the source database |

## Project layout

```
.
├── pyproject.toml                 # Project definition, dependencies, tool configs
├── databricks.yml                 # Databricks Asset Bundle definition
├── docker-compose.yml             # Local Postgres (wal_level=logical) for integration tests
├── .github/workflows/             # CI (lint/type/test) and CD (bundle deploy) pipelines
├── src/lakeflow_sync/
│   ├── pipeline_main.py           # CLI / job entry point
│   ├── full_load.py               # Full Load pipeline logic
│   ├── cdc_load.py                # CDC incremental pipeline logic
│   ├── quality_checks.py          # Post-load data quality gate
│   ├── resilience.py              # Retry/backoff + failure classification
│   ├── utils/                     # Logging, notifications
│   └── pg_stream/                 # Append-only CDC event normalization
│       └── source_compat.py       # Import-safety shim for dlt.sources.pg_replication
├── tests/                         # Unit tests (mocked, no external services)
│   └── integration/               # End-to-end CDC test against real Postgres logical replication
├── scripts/                       # simulate / verify / quality-check / run_integration_tests.sh
├── resources/jobs/                # Databricks Lakeflow Job resource definitions
└── .dlt/                          # Local dlt config / secrets (gitignored)
```

## Known limitations

- **Serverless network egress:** on constrained free-tier Databricks Serverless compute, connections to Unity Catalog Volumes storage endpoints can be blocked, surfacing as `Connection refused`. Workaround: run locally (Quick Start above) or on classic compute.
- **Single replication slot per environment:** running multiple concurrent CDC jobs against the same slot will cause contention — scale by adding slots/publications per table group if needed.
- **`dlt.sources.pg_replication` is a verified source, not a guaranteed stable import.** `dlt` historically ships this to be vendored via `dlt init pg_replication databricks` rather than imported directly like `sql_database`. This project imports it directly behind [`source_compat.get_replication_source()`](src/lakeflow_sync/pg_stream/source_compat.py), which fails with a clear, actionable error rather than a bare `ModuleNotFoundError` if a given `dlt` version doesn't expose it — but it hasn't been re-verified against every `dlt` release.
- **Retry classification is message-based, not error-code-based.** `resilience.classify_error` matches substrings in the exception string (see [Resilience](#resilience--retry-behavior)) — deliberately conservative, since unrecognized errors default to "transient" and get retried rather than failing instantly, but a Postgres/`dlt` version that changes its error wording could be misclassified until the marker lists are updated.
- **Integration coverage is CDC-happy-path only.** `tests/integration/test_cdc_end_to_end.py` proves one insert/update/delete cycle round-trips correctly through a live replication slot; it doesn't yet cover concurrent writers, large batches, mid-stream schema changes, or an actual slot invalidation end-to-end — those are covered at the unit level (`tests/test_cdc_load.py`, `tests/test_resilience.py`) with mocks, not against real Postgres.

## Ideas for further enhancement

- Swap the webhook notifier for a native Databricks SQL Alert on the quality-gate table.
- Add schema-drift detection that fails fast (rather than silently evolving) for a configurable list of protected columns.
- Partition the CDC append table by `_cdc_committed_at` date for cheaper time-bound queries at scale.

## License

MIT — see [LICENSE](LICENSE).
