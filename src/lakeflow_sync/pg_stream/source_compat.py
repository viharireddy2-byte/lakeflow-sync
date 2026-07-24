"""Compatibility shim around dlt's Postgres replication source.

`pg_replication` ships from dlt as a **verified source**: historically that
means it's meant to be vendored into your project with
`dlt init pg_replication <destination>` (which copies the source code
locally, the way the blueprint this project is based on does under
`pg_replication/`), rather than guaranteed as a stable, directly-importable
module the way `sql_database` became in dlt 1.0+.

Importing `dlt.sources.pg_replication` directly (as `cdc_load.py` does)
works against recent `dlt` releases that expose it, but that is a
version-dependent assumption, not a guarantee. This module isolates that
one import behind a clear, actionable error instead of letting an
incompatible `dlt` version surface as a bare `ModuleNotFoundError` deep
inside `cdc_load.py`'s stack trace.

Nothing about the happy path changes: when the import succeeds,
`get_replication_source()` returns the exact same `replication_source`
callable `cdc_load.py` used before.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

try:
    from dlt.sources.pg_replication import (  # type: ignore[import-not-found]
        replication_source as _replication_source,
    )

    PG_REPLICATION_IMPORT_ERROR: Optional[ImportError] = None
except ImportError as exc:  # pragma: no cover - only hit on incompatible dlt versions
    _replication_source = None
    PG_REPLICATION_IMPORT_ERROR = exc


def get_replication_source() -> Callable[..., Any]:
    """Return dlt's `replication_source`, or raise a clear, actionable error.

    If this raises, the fix is one of:

    1. Vendor the verified source into this project instead of relying on
       the direct import::

           uv run dlt init pg_replication databricks

       This copies dlt's `pg_replication` source into
       `src/lakeflow_sync/pg_replication/` (mirroring how the reference
       blueprint ships it). Update the import in `cdc_load.py` from
       ``from lakeflow_sync.pg_stream.source_compat import get_replication_source``
       to importing `replication_resource` from the vendored package.
    2. Pin a `dlt` version confirmed to expose
       `dlt.sources.pg_replication` as a direct import, and re-run
       `uv sync`.
    """
    if _replication_source is None:
        raise ImportError(
            "dlt.sources.pg_replication is not importable with the installed "
            f"dlt version ({PG_REPLICATION_IMPORT_ERROR}). pg_replication is "
            "shipped by dlt as a 'verified source', which historically means "
            "it's meant to be vendored via `dlt init pg_replication "
            "databricks` rather than guaranteed as a stable import. See "
            "get_replication_source()'s docstring for how to fix this."
        ) from PG_REPLICATION_IMPORT_ERROR
    return _replication_source
