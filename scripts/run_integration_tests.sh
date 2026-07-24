#!/usr/bin/env bash
# Bring up local Postgres (logical replication enabled), run the CDC
# end-to-end integration test against a local duckdb file as the
# destination (no real Databricks workspace required), then tear down.
#
# This is separate from `uv run pytest` (the CI quality gate) on purpose --
# it needs Docker and takes longer, so it's opt-in for local dev / a
# dedicated CI job, not part of every push.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Installing integration test extras (duckdb)..."
uv sync --all-extras

echo "Starting Postgres (logical replication)..."
docker compose up -d --wait postgres

export LAKEFLOW_SYNC_PG_DSN="postgresql://lakeflow_sync:lakeflow_sync@localhost:55432/lakeflow_sync"
export LAKEFLOW_SYNC_DESTINATION="duckdb"

cleanup() {
  echo "Stopping Postgres..."
  docker compose down -v
}
trap cleanup EXIT

echo "Running integration tests..."
uv run pytest tests/integration -v
