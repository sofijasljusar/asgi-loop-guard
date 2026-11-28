from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypeVar
from urllib.error import URLError
from urllib.request import urlopen

T = TypeVar("T")


class OverloadError(Exception):
    """Raised when bounded concurrency or queue capacity is exceeded."""


class UpstreamTimeoutError(Exception):
    """Raised when an upstream call exceeds its deadline."""


class UpstreamRequestError(Exception):
    """Raised when a real upstream HTTP request fails."""


@dataclass(slots=True)
class Guardrails:
    active_async: int
    rejected_async: int
    active_threads: int
    queued_threads: int
    rejected_threads: int
    timed_out_calls: int


class BoundedExecutor:
    """Fixed-size thread pool with a hard admission cap
    (rejects access instead of unbounded queueing)."""
    def __init__(self, max_workers: int, max_queued: int) -> None:
        self._max_workers = max_workers
        self._max_queued = max_queued
        self._max_in_flight = max_workers + max_queued
        self._in_flight = 0
        self._active = 0
        self._rejected = 0
        self._timed_out = 0
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="elg-worker",
        )

    async def run(
            self,
            fn: Callable[..., T],
            *args: object,
            timeout_s: float,
    ) -> T:
        async with self._lock:
            if self._in_flight >= self._max_in_flight:
                self._rejected += 1
                raise OverloadError("Thread admission limit reached")
            self._in_flight +=1

        loop = asyncio.get_running_loop()
        started_on_worker = False

        async def _worker_wrapper() -> T:
            nonlocal started_on_worker
            async with self._lock:
                self._active += 1
                started_on_worker = True
            try:
                return fn(*args)
            finally:
                async with self._lock:
                    self._active -= 1
        try:
            async with asyncio.timeout(timeout_s):
                return await loop.run_in_executor(self._executor, lambda: fn(*args))
        except TimeoutError as exc:
            async with self._lock:
                self._timed_out += 1
            raise UpstreamTimeoutError(f"Timed out after {timeout_s}s") from exc
        except asyncio.CancelledError:
            raise
        finally:
            async with self._lock:
                self._in_flight = max (0, self._in_flight - 1)
                if started_on_worker:
                    self._active = max(0, self._active - 1)

    async def run_tracked(
            self,
            fn: Callable[[], T],
            *,
            timeout_s: float,
    ) -> T:
        async with self._lock:
            if self._in_flight >= self._max_in_flight:
                self._rejected += 1
                raise OverloadError("Thread admission limit reached.")
            self._in_flight += 1

        loop = asyncio.get_running_loop()

        def _guarded_call() -> T:
            return fn()

        try:
            async with asyncio.timeout(timeout_s):
                async with self._active_slot():
                    return await loop.run_in_executor(self._executor, _guarded_call)
        except TimeoutError as exc:
            async with self._lock:
                self._timed_out += 1
            raise UpstreamTimeoutError(f"Timed out after {timeout_s}s") from exc
        finally:
            async with self._lock:
                self._in_flight = max(0, self._in_flight - 1)


    @asynccontextmanager
    async def _active_slot(self):
        async with self._lock:
            self._active += 1
        try:
            yield
        finally:
            async with self._lock:
                self._active = max(0, self._active - 1)

    async def stats(self) -> tuple[int, int, int, int]:
        async with self._lock:
            queued = max(0, self._in_flight - self._timed_out)
            return self._active, queued, self._rejected, self._timed_out

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


class AsyncConcurrencyGate:
    """Bounds concurrent async calls with a semaphore and non-blocking admission."""
    def __init__(self, max_concurrent: int) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active = 0
        self._rejected = 0
        self._timed_out = 0
        self._lock = asyncio.Lock()

    async def acquire_or_reject(self) -> bool:
        async with self._lock:
            if self._semaphore.locked():
                self._rejected += 1
                return False
            await self._semaphore.acquire()
            self._active += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            self._semaphore.release()

    async def record_timeout(self) -> None:
        async with self._lock:
            self._timed_out += 1

    async def stats(self) -> tuple[int, int, int]:
        async with self._lock:
            return self._active, self._rejected, self._timed_out


def blocking_http_get(url: str, timeout_s: float) -> dict[str, object]:
    """Deliberately synchronous/blocking HTTP request using stdlib urllib."""

    try:
        with urlopen(url, timeout=timeout_s) as resp:  # noqa: S310
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except (URLError, TimeoutError, OSError) as exc:
        raise UpstreamRequestError(str(exc)) from exc

