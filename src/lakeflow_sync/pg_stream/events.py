"""Normalize raw logical-replication change events into append-only rows.

`dlt.sources.pg_replication` yields one dict per WAL change with keys like
`{"data": {...}, "action": "insert" | "update" | "delete", "lsn": ..., ...}`
depending on the plugin (`pgoutput` / `wal2json`) configured on the
publication. This module converts that into a stable, storage-friendly
shape so downstream Delta tables have a consistent schema regardless of
which underlying decoding plugin produced the event.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any


class CdcOperation(str, enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


def normalize_change_event(raw_event: dict[str, Any]) -> dict[str, Any]:
    """Convert one raw replication-slot event into a flat, append-only record.

    Parameters
    ----------
    raw_event:
        A single event as produced by the upstream replication source, e.g.::

            {
                "action": "update",
                "data": {"id": 42, "status": "shipped"},
                "lsn": "0/16B3748",
                "commit_ts": "2026-07-24T10:15:00Z",
            }

    Returns
    -------
    A flat dict ready to be yielded to `dlt` for an append-only Delta write.
    Every record carries three CDC metadata columns so consumers downstream
    (dbt models, analysts) can reconstruct history without needing a
    separate audit table:

    - `_cdc_operation`: one of "insert" / "update" / "delete"
    - `_cdc_lsn`: the WAL log sequence number, useful for ordering/dedup
    - `_cdc_committed_at`: UTC timestamp the change was committed on Postgres
    """
    action = str(raw_event.get("action", "")).lower()
    try:
        operation = CdcOperation(action)
    except ValueError as exc:
        raise ValueError(f"Unrecognized CDC action '{action}' in event: {raw_event}") from exc

    data = dict(raw_event.get("data") or {})
    committed_at = raw_event.get("commit_ts")
    if committed_at is None:
        committed_at = datetime.now(timezone.utc).isoformat()

    normalized = {
        **data,
        "_cdc_operation": operation.value,
        "_cdc_lsn": raw_event.get("lsn"),
        "_cdc_committed_at": committed_at,
    }
    return normalized
