from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from eventloop_guard.config import settings
from eventloop_guard.guardrails import (
    AsyncConcurrencyGate,
    BoundedExecutor,
    OverloadError,
    UpstreamRequestError,
    UpstreamTimeoutError,
    blocking_http_get,
)
from eventloop_guard.lag import EventLoopMonitor
from eventloop_guard.upstream import LocalUpstreamServer


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    lag_monitor = EventLoopMonitor(
        interval_ms=settings.lag_interval_ms,
        history_size=settings.lag_history_size,
    )
    upstream = LocalUpstreamServer(
        host=settings.upstream_host,
        port=settings.upstream_port,
    )
    async_gate = AsyncConcurrencyGate(max_concurrent=settings.max_concurrent_upstream)
    thread_pool = BoundedExecutor(
        max_workers=settings.max_thread_workers,
        max_queued=settings.max_queued_threads,
    )
    http_client = httpx.AsyncClient(
        base_url=upstream.base_url,
        limits=httpx.Limits(
            max_connections=settings.max_concurrent_upstream,
            max_keepalive_connections=settings.max_concurrent_upstream,
        ),
    )

    app.state.lag_monitor = lag_monitor
    app.state.upstream = upstream
    app.state.async_gate = async_gate
    app.state.thread_pool = thread_pool
    app.state.http_client = http_client

    await upstream.start()
    await lag_monitor.start()
    try:
        yield
    finally:
        await lag_monitor.stop()
        await http_client.aclose()
        thread_pool.close()
        await upstream.stop()

app = FastAPI(title="eventloop-guard", version="0.1.0", lifespan=lifespan)


@app.exception_handler(OverloadError)
async def overload_handler(_request: Request, exc: OverloadError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"error": "service_overloaded", "detail": str(exc)},
        headers={"Retry-After": str(settings.retry_after_s)},
    )


@app.exception_handler(UpstreamTimeoutError)
async def timeout_handler(_request: Request, exc: UpstreamTimeoutError) -> JSONResponse:
    return JSONResponse(
        status_code=504,
        content={"error": "upstream_timeout", "detail": str(exc)},
    )


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    snap = request.app.state.lag_monitor.snapshot()
    return {
        "status": "ok",
        "event_loop_lag_ms": {
            "last": snap.last_ms,
            "p95": snap.p95_ms,
            "max": snap.max_ms,
        },
    }


@app.get("/io/async")
async def io_async(
        request: Request,
        delay_ms: int = Query(default=150, ge=0, le=5000),
        timeout_s: float = Query(default=settings.default_timeout_s, gt=0.05, le=10.0),
) -> dict[str, object]:
    gate: AsyncConcurrencyGate = request.app.state.async_gate
    client: httpx.AsyncClient = request.app.state.http_client

    if not await gate.acquire_or_reject():
        raise OverloadError("Async concurrency limit reached")

    start = time.perf_counter()
    try:
        async with asyncio.timeout(timeout_s):
            response = await client.get("/work", params={"delay_ms": delay_ms})
            response.raise_for_status()
            data = response.json()
    except TimeoutError as exc:
        await gate.record_timeout()
        raise UpstreamTimeoutError(f"Async upstream call exceeded {timeout_s}s") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream error: {exc}") from exc
    finally:
        await gate.release()

    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 2)
    return {"mode": "async_non_blocking", "elapsed_ms": elapsed_ms, "upstream": data}


@app.get("/io/sync-in-async")
async def io_sync_in_async(
        request: Request,
        delay_ms: int = Query(default=150, ge=0, le=5000),
        timeout_s: float = Query(default=settings.default_timeout_s, gt=0.05, le=10.0),
) -> dict[str, object]:
    """Intentional anti-pattern: blocking I/O executed directly on the event loop thread."""
    upstream: LocalUpstreamServer = request.app.state.upstream
    url = f"{upstream.base_url}/work?delay_ms={delay_ms}"

    start = time.perf_counter()
    try:
        # Blocks the single event loop thread while waiting on socket read
        data = blocking_http_get(url, timeout_s=timeout_s)
    except UpstreamRequestError as exc:
        raise HTTPException(status_code=502, detail=f"Blocking upstream error: {exc}") from exc

    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 2)
    return {"mode": "sync_blocking_on_loop", "elapsed_ms": elapsed_ms, "upstream": data}


@app.get("/io/guarded-sync")
async def io_guarded_sync(
        request: Request,
        delay_ms: int = Query(default=settings.default_timeout_s, gt=0, le=5000),
        timeout_s: float = Query(default=settings.default_timeout_s, gt=0.05, le=10.0),
) -> dict[str, object]:
    """Offloads blocking I/O to a bounded thread pool with hard admission cap and timeout."""
    upstream: LocalUpstreamServer = request.app.state.upstream
    pool: BoundedExecutor = request.app.state.thread_pool
    url = f"{upstream.base_url}/work?delay_ms={delay_ms}"

    start = time.perf_counter()
    try:
        data = await pool.run_tracked(
            lambda: blocking_http_get(url, timeout_s=timeout_s),
            timeout_s=timeout_s,
        )
    except UpstreamRequestError as exc:
        raise HTTPException(status_code=502, detail=f"Guarded upstream error: {exc}") from exc

    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 2)
    return {"mode": "bounded_thread_pool", "elapsed_ms": elapsed_ms, "upstream": data}


@app.get("/cpu/work")
async def cpu_work(
        iterations: int = Query(default=150_000, ge=10_000, le=2_000_000),
) -> dict[str, object]:
    """Synchronous CPU-bound work on the loop thread to illustrate GIL & single-worker limits."""
    start = time.perf_counter()
    digest = hashlib.pbkdf2_hmac("sha256", b"secret-key", b"salt", iterations)
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 2)
    return {"mode": "cpu_bound", "iterations": iterations, "elapsed_ms": elapsed_ms, "hash": digest.hex()[:12]}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request) -> str:
    snap = request.app.state.lag_monitor.snapshot()
    active_async, rejected_async, timed_out_async = await request.app.state.async_gate.stats()
    active_threads, queued_threads, rejected_threads, timed_out_threads = (
        await request.app.state.thread_pool.stats()
    )

    lines = [
        f"elg_loop_lag_ms_last {snap.last_ms}",
        f"elg_loop_lag_ms_p50 {snap.p50_ms}",
        f"elg_loop_lag_ms_p95 {snap.p95_ms}" ,
        f"elg_loop_lag_ms_p99 {snap.p99_ms}",
        f"elg_loop_lag_ms_max {snap.max_ms}",
        f"elg_async_active {active_async}",
        f"elg_async_rejected_total {rejected_async}",
        f"elg_async_timed_out_total {timed_out_async}",
        f"elg_threads_active {active_threads}",
        f"elg_threads_queued {queued_threads}",
        f"elg_threads_rejected_total {rejected_threads}",
        f"elg_threads_timed_out_total {timed_out_threads}",
    ]
    return "\n".join(lines) + "\n"
