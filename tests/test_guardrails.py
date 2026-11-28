from __future__ import annotations

import asyncio

import httpx
from fastapi.testclient import TestClient

from eventloop_guard.app import app


def test_health_and_metrics_endpoints() -> None:
    with TestClient(app) as client:
        health_resp = client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json()["status"] == "ok"

        metrics_resp = client.get("/metrics")
        assert metrics_resp.status_code == 200
        assert "elg_loop_lag_ms_p95" in metrics_resp.text


def test_io_to_async_succeeds() -> None:
    with TestClient(app) as client:
        resp = client.get("/io/async", params={"delay_ms": 20, "timeout_s": 2.0})
        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "async_non_blocking"
        assert body["upstream"]["ok"] is True


def test_io_async_times_out_cleanly() -> None:
    with TestClient(app) as client:
        resp = client.get("/io/async", params={"delay_ms": 500, "timeout_s": 0.1})
        assert resp.status_code == 504
        assert resp.json()["error"] == "upstream_timeout"


def tests_guarded_sync_rejects_excess_load_with_503() -> None:
    async def _run() -> list[int]:
        transport = httpx.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                tasks = [
                    client.get("/io/guarded-sync", params={"delay_ms": 250, "timeout_s": 2.0})
                    for _ in range(80)
                ]
                responses = await asyncio.gather(*tasks)
                return [r.status_code for r in responses]

    statuses = asyncio.run(_run())
    assert 200 in statuses
    assert 503 in statuses
