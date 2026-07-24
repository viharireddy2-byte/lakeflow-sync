"""Retry and failure-classification helpers around dlt pipeline runs.

Addresses a gap in the original blueprint: pipeline execution failures were
either all retried forever or all fatal, with no distinction between:

- **transient** errors (connection reset, lock timeout, momentary network
  blip) -- worth a bounded number of retries with backoff.
- **permanent** errors (bad credentials, missing table, permission denied)
  -- retrying cannot fix these; fail fast and surface clearly.
- **replication-slot-invalidated** errors -- a CDC-specific permanent
  failure (the slot was dropped, or Postgres has already recycled the WAL
  segments the slot needed). Retrying is actively harmful here: it either
  loops forever or silently skips changes. This needs its own signal so a
  caller (or on-call engineer) knows the fix is "re-create the slot and run
  a fresh full_load", not "just retry the job".

`run_with_retry` wraps a zero-arg callable (typically `pipeline.run(...)`)
and applies this policy. It changes nothing about the *successful* path --
a call that succeeds on the first attempt behaves exactly as if it had been
called directly.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, TypeVar

from lakeflow_sync.utils.logger import get_logger, log_with_context

logger = get_logger(__name__)

T = TypeVar("T")


class ReplicationSlotInvalidatedError(RuntimeError):
    """The Postgres replication slot is gone, or its WAL has been recycled.

    This is a *permanent* failure for the current slot: retrying will not
    help. Operationally, recovery means either:

    1. Re-creating the replication slot/publication and running a fresh
       `full_load` before resuming CDC, or
    2. Increasing WAL retention (`wal_keep_size`, or the slot's
       `max_slot_wal_keep_size`) so CDC has more time to catch up before the
       slot's WAL is reclaimed.
    """


# Each entry is a tuple of substrings that must ALL appear (lowercased) in
# the error message for it to count as a replication-slot-invalidated
# failure. Multiple entries are OR'd together. Based on real Postgres error
# text for a dropped slot ("replication slot ... does not exist") and a
# slot whose WAL has already been recycled ("requested WAL segment ... has
# already been removed").
_SLOT_INVALIDATED_SIGNATURES: tuple[tuple[str, ...], ...] = (
    ("replication slot", "does not exist"),
    ("replication slot", "invalidated"),
    ("requested wal segment",),
    ("wal segment", "already been removed"),
)

# Substrings that indicate a permanent, non-retryable configuration/auth
# problem unrelated to the replication slot.
_PERMANENT_MARKERS = (
    "authentication failed",
    "permission denied",
    "password authentication failed",
    "no such table",
    "relation \"",  # e.g. relation "orders" does not exist
    "catalog not found",
    "schema not found",
    "invalid credentials",
)


def classify_error(exc: BaseException) -> str:
    """Classify an exception as "slot_invalidated", "permanent", or "transient".

    Best-effort, message-based classification. Anything not recognized as
    slot-invalidated or permanent defaults to "transient" -- this is a
    deliberate choice: an unrecognized error still gets a small, bounded
    number of retries rather than either failing instantly (which could
    turn a one-off network blip into a job failure) or retrying forever.
    """
    message = str(exc).lower()

    if any(
        all(term in message for term in signature) for signature in _SLOT_INVALIDATED_SIGNATURES
    ):
        return "slot_invalidated"

    if any(marker in message for marker in _PERMANENT_MARKERS):
        return "permanent"

    return "transient"


def run_with_retry(
    func: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay_seconds: float = 2.0,
    op_name: str = "operation",
    sleep: Optional[Callable[[float], None]] = None,
) -> T:
    """Run `func`, retrying transient failures with exponential backoff.

    - `slot_invalidated` errors are wrapped in `ReplicationSlotInvalidatedError`
      and raised immediately -- never retried.
    - `permanent` errors are re-raised immediately, unmodified -- never retried.
    - `transient` errors are retried up to `max_attempts` times total, with
      delay `base_delay_seconds * 2**(attempt - 1)` between attempts.

    `sleep` is injectable purely for testing so unit tests don't have to
    wait through real backoff delays.
    """
    last_exc: BaseException | None = None
    sleep_fn = sleep if sleep is not None else time.sleep

    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - intentionally broad; classified below
            last_exc = exc
            classification = classify_error(exc)

            if classification == "slot_invalidated":
                log_with_context(
                    logger,
                    40,
                    f"{op_name} failed: replication slot invalidated (not retrying)",
                    attempt=attempt,
                    error=str(exc),
                )
                raise ReplicationSlotInvalidatedError(str(exc)) from exc

            if classification == "permanent":
                log_with_context(
                    logger,
                    40,
                    f"{op_name} failed with a permanent error (not retrying)",
                    attempt=attempt,
                    error=str(exc),
                )
                raise

            if attempt == max_attempts:
                log_with_context(
                    logger,
                    40,
                    f"{op_name} failed after {attempt} attempt(s), giving up",
                    error=str(exc),
                )
                raise

            delay = base_delay_seconds * (2 ** (attempt - 1))
            log_with_context(
                logger,
                30,
                f"{op_name} failed with a transient error, retrying",
                attempt=attempt,
                max_attempts=max_attempts,
                retry_in_seconds=delay,
                error=str(exc),
            )
            sleep_fn(delay)

    # Unreachable in practice (the loop always returns or raises), but keeps
    # static type checkers happy about the return type.
    assert last_exc is not None
    raise last_exc
