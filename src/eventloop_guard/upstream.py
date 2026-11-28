from __future__ import annotations

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class LocalUpstreamServer:
    """Real local HTTP server on 127.0.0.1 for realistic socket and timeout behavior."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8001) -> None:
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread_task: asyncio.Task[None] | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                delay_ms = int(query.get("delay_ms", ["100"])[0])
                status_code = int(query.get("status", ["200"])[0])

                if delay_ms > 0:
                    import time

                    time.sleep(min(delay_ms, 10_000) / 1000.0)

                payload = (json.dumps({"ok": status_code < 400, "delay_ms": delay_ms})
                           .encode("utf-8"))
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object):  # noqa: A002
                # Silence noisy stdlib HTTP access logs during load tests
                return

        return Handler

    async def start(self) -> None:
        if self._server is not None:
            return
        server = ThreadingHTTPServer((self.host, self.port), self._build_handler())
        server.daemon_threads = True
        self._server = server
        loop = asyncio.get_running_loop()
        self._thread_task = loop.create_task(
            asyncio.to_thread(server.serve_forever),
            name="local-upstream-server",
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread_task is not None:
            try:
                await self._thread_task
            except asyncio.CancelledError:
                pass
            self._thread_task = None
