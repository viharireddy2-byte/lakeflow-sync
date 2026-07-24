"""Custom PostgreSQL logical-replication CDC source for dlt.

This module wraps `dlt.sources.pg_replication` (the community-maintained
dltHub verified source) and forces **append-only** semantics on top of it.

Why append-only instead of merge?
----------------------------------
dlt's stock replication source is built to *mirror* the source table --
an UPDATE overwrites the row, a DELETE removes it. That is the right
default for an operational replica, but it is the wrong default for a
Bronze layer in a Lakehouse: once a row is overwritten or deleted, the
history of what happened is gone.

Here we instead treat every WAL event (insert / update / delete) as an
immutable fact appended to the Bronze table, tagged with the operation
type and a commit timestamp. `full_load.py` still produces the initial
authoritative snapshot; `cdc_load.py` (via this module) then appends the
ongoing event log on top of it.
"""

from lakeflow_sync.pg_stream.events import CdcOperation, normalize_change_event

__all__ = ["CdcOperation", "normalize_change_event"]
