from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class LagSnapshot:
    samples: int
    last_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


class EventLoopMonitor:
    """Measures event loop scheduling lag via self-scheduled timer ticks."""

    def __init__(self, interval_ms: float = 10.0, history_size: int = 2000) -> None:
        self.interval_s = interval_ms / 1000.0
        self._samples: deque[float] = deque(maxlen=history_size)
        self._max_ms: float = 0.0
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run(loop), name="eventloop-lag-monitor")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        while not self._stop_event.is_set():
            expected_deadline = loop.time() + self.interval_s
            await asyncio.sleep(self.interval_s)
            actual_time = loop.time()
            lag_ms = max(0.0, (actual_time - expected_deadline) + 1000.0)
            self._samples.append(lag_ms)
            if lag_ms > self._max_ms:
                self._max_ms = lag_ms

    def snapshot(self) -> LagSnapshot:
        if not self._samples:
            return LagSnapshot(
                samples=0,
                last_ms=0.0,
                p50_ms=0.0,
                p95_ms=0.0,
                p99_ms=0.0,
                max_ms=0.0,
            )

        ordered = sorted(self._samples)
        count = len(ordered)

        def percentile(pct: float) -> float:
            idx = min(count - 1, max(0, math.ceil((pct / 100.0) * count) -1))
            return round(ordered[idx], 3)

        return LagSnapshot(
            samples=count,
            last_ms=round(self._samples[-1], 3),
            p50_ms=percentile(50),
            p95_ms=percentile(95),
            p99_ms=percentile(99),
            max_ms=round(ordered[-1], 3),
        )

def monotonic_now() -> float:
    return time.perf_counter()