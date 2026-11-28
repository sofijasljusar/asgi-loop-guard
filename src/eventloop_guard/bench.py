from __future__ import annotations

import argparse
import asyncio
import math
import time
from dataclasses import dataclass


import httpx


@dataclass(slots=True)
class ScenarioResult:
    name: str
    total_requests: int
    ok_200: int
    overloaded_503: int
    errors: int
    duration_s: float
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    health_max_ms: float
    loop_lag_p95_ms: float
    loop_lag_max_ms: float


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil((pct / 100.0) * len(ordered)) -1))
    return round(ordered[idx], 2)


async def run_scenario(
    base_url: str,
    name: str,
    path: str,
    *,
    concurrency: int,
    total_requests: int,
) -> ScenarioResult:
    latencies_ms: list[float] = []
    health_ms: list[float] = []
    ok_200 = 0
    overloaded_503 = 0
    errors = 0

    queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(total_requests):
        queue.put_nowait(i)

    stop_health = asyncio.Event()

    async def probe_health() -> None:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as hclient:
            while not stop_health.is_set():
                t0 = time.perf_counter()
                try:
                    resp = await hclient.get("/health")
                    if resp.status_code == 200:
                        health_ms.append((time.perf_counter() - t0) * 1000.0)
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05)

    async def worker() -> None:
        nonlocal ok_200, overloaded_503, errors
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                t0 = time.perf_counter()
                try:
                    resp = await client.get(path)
                    elapsed = (time.perf_counter() - t0) * 1000.0
                    latencies_ms.append(elapsed)
                    if resp.status_code == 200:
                        ok_200 += 1
                    elif resp.status_code == 503:
                        overloaded_503 += 1
                    else:
                        errors += 1
                except httpx.HTTPError:
                    errors += 1
                finally:
                    queue.task_done()

    health_task = asyncio.create_task(probe_health())
    start = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    duration_s = max(0.001, time.perf_counter() - start)
    stop_health.set()
    await health_task

    lag_p95 = 0.0
    lag_max = 0.0
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as mclient:
        mresp = await mclient.get("/metrics")
        for line in mresp.text.splitlines():
            if line.startswith("elg_loop_lag_ms_p95 "):
                lag_p95 = float(line.split()[1])
            elif line.startswith("elg_loop_lag_ms_max "):
                lag_max = float(line.split()[1])

    return ScenarioResult(
        name=name,
        total_requests=total_requests,
        ok_200=ok_200,
        overloaded_503=overloaded_503,
        errors=errors,
        duration_s=round(duration_s, 3),
        rps=round(total_requests / duration_s, 2),
        p50_ms=_percentile(latencies_ms, 50),
        p95_ms=_percentile(latencies_ms, 95),
        p99_ms=_percentile(latencies_ms, 99),
        health_max_ms=round(max(health_ms), 2) if health_ms else 0.0,
        loop_lag_p95_ms=lag_p95,
        loop_lag_max_ms=lag_max,
    )


async def main(base_url: str, concurrency: int, requests_count: int) -> None:
    scenarios = [
        ("async-non-blocking", "/io/async?delay_ms=150"),
        ("sync-in-async-trap", "/io/sync-in-async?delay_ms=150"),
        ("guarded-thread-pool", "/io/guarded-sync?delay_ms=150"),
    ]

    results: list[ScenarioResult] = []
    for name, path in scenarios:
        res = await run_scenario(
            base_url,
            name,
            path,
            concurrency=concurrency,
            total_requests=requests_count,
        )
        results.append(res)
        await asyncio.sleep(0.5)

    header = (
        f"{'Scenario':<22} {'RPS':>7} {'200':>5} {'503':>5} {'p50(ms)':>8}"
        f"{'p95(ms)':>8} {'p99(ms)':>8} {'/health max':>12} {'lag p95':>8}"
    )

    print("\n" + header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.name:<22} {r.rps:>7.2f} {r.ok_200:>5} {r.overloaded_503:>5} "
            f"{r.p50_ms:>8.1f} {r.p95_ms:>8.1f} {r.p99_ms:>8.1f} "
            f"{r.health_max_ms:>11.1f}ms {r.loop_lag_p95_ms:>7.1f}ms"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--concurrency", type=int, default=60)
    parser.add_argument("--requests", type=int, default=120)
    args = parser.parse_args()
    asyncio.run(main(args.base_url, args.concurrency, args.requests))
