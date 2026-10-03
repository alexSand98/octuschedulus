import json
import logging
import threading
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from shared.logging import log_stage

logger = logging.getLogger("consumer.internal_api")

STATUS_PATH = "/internal/status"


def start_status_server(port: int, get_status: Callable[[], dict]) -> None:
    """Serve GET /internal/status from a background thread (Compose-internal port only)."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != STATUS_PATH:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            try:
                status = get_status()
            except Exception as exc:
                log_stage(logger, "status.failed", "Could not build status", level=logging.ERROR, error=str(exc))
                self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "status unavailable"})
                return
            self._send(HTTPStatus.OK, status)

        def _send(self, status: HTTPStatus, payload: dict) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            pass  # polled every few seconds by the rollout controller

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="internal-api", daemon=True).start()
