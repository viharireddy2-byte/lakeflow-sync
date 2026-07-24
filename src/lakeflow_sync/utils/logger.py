"""Structured logging helpers.

Enhancement over a plain `logging.basicConfig` setup: every log line is
emitted as a single-line JSON object, which makes it trivially searchable
in Databricks job run logs / any log aggregator (Datadog, CloudWatch, etc).
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any


class JsonFormatter(logging.Formatter):
    """Render each LogRecord as one JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(time.time(), 3),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)  # type: ignore[attr-defined]
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a module-level logger configured with the JSON formatter.

    Safe to call repeatedly (e.g. once per module import) -- handlers are
    only attached once per logger name.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_with_context(logger: logging.Logger, level: int, message: str, **context: Any) -> None:
    """Log `message` at `level` with arbitrary structured `context` fields attached."""
    logger.log(level, message, extra={"extra_fields": context})
