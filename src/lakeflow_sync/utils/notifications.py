"""Run-outcome notifications.

Enhancement not present in the baseline blueprint this project is inspired
by: on pipeline completion (success or failure) we optionally POST a small
JSON summary to a Slack-compatible incoming webhook, so failures in a
Lakeflow Job surface immediately instead of requiring someone to check the
Jobs UI.

Configuring `LAKEFLOW_SYNC_WEBHOOK_URL` is entirely optional -- if it is
unset, `notify_run_result` is a no-op.
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

from lakeflow_sync.utils.logger import get_logger

logger = get_logger(__name__)


def notify_run_result(
    mode: str,
    status: str,
    rows_processed: int,
    duration_seconds: float,
    error: str | None = None,
) -> None:
    """Send a best-effort notification about a pipeline run.

    Never raises -- a notification failure must not fail the pipeline.
    """
    webhook_url = os.environ.get("LAKEFLOW_SYNC_WEBHOOK_URL")
    if not webhook_url:
        return

    emoji = "✅" if status == "success" else "🚨"
    text = (
        f"{emoji} lakeflow-sync [{mode}] finished with status={status} "
        f"rows={rows_processed} duration={duration_seconds:.1f}s"
    )
    if error:
        text += f"\nerror: {error}"

    payload: dict[str, Any] = {"text": text}
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=5):  # noqa: S310
            pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to send run notification: %s", exc)
