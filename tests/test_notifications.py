import os
from unittest import mock

from lakeflow_sync.utils.notifications import notify_run_result


def test_notify_is_noop_without_webhook_url() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        # Should not raise even though no webhook is configured.
        notify_run_result("full_load", "success", 100, 1.23)


def test_notify_swallows_delivery_errors() -> None:
    with mock.patch.dict(
        os.environ, {"LAKEFLOW_SYNC_WEBHOOK_URL": "https://example.invalid/webhook"}, clear=True
    ):
        # Delivery will fail (no network / invalid host) but must not raise.
        notify_run_result("cdc", "failure", 0, 0.5, error="boom")
