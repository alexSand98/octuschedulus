import json
import logging
import secrets
import signal
import threading
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from routing import PERCENT_KEY, PRERELEASE, STABLE, Router
from shared.config import ProducerSettings
from shared.events import JOB_ID_PATTERN, JobEvent
from shared.logging import log_stage, setup_logging

logger = logging.getLogger("producer")

EVENTS_PUBLISHED = Counter(
    "events_published_total", "Job events published to the stream", ["type", "status"]
)
PUBLISH_DURATION = Histogram("publish_duration_seconds", "Time spent in XADD")
EVENTS_ROUTED = Counter("events_routed_total", "Job events routed per variant", ["variant"])
AB_MODE_ENABLED = Gauge("ab_mode_enabled", "1 if A/B routing is enabled")
PRERELEASE_TRAFFIC_PERCENT = Gauge(
    "prerelease_traffic_percent", "Pre-release traffic percent last read from Redis"
)

MAX_BODY_BYTES = 64 * 1024


class JobRequest(BaseModel):
    """Body of POST /jobs. Both fields are optional; jobId is generated when omitted."""

    model_config = ConfigDict(extra="forbid")

    jobId: str | None = Field(default=None, pattern=JOB_ID_PATTERN)
    type: str = "http"


class PublishError(Exception):
    pass


class Publisher:
    def __init__(self, client: redis.Redis, router: Router) -> None:
        self.client = client
        self.router = router

    def publish(self, request: JobRequest) -> tuple[JobEvent, str]:
        event = JobEvent(
            jobId=request.jobId or secrets.token_hex(4),
            type=request.type,
            createdAt=datetime.now(timezone.utc),
            traceId=str(uuid.uuid4()),
        )
        ids = {"jobId": event.jobId, "traceId": event.traceId}
        try:
            route = self.router.route(event.jobId)
            PRERELEASE_TRAFFIC_PERCENT.set(route.percent)
            EVENTS_ROUTED.labels(variant=route.variant).inc()
            log_stage(logger, "event.routed", "Event routed", variant=route.variant,
                      stream=route.stream, bucket=route.bucket, percent=route.percent, **ids)
            with PUBLISH_DURATION.time():
                message_id = self.client.xadd(route.stream, event.to_fields())
        except redis.RedisError as exc:
            EVENTS_PUBLISHED.labels(type=event.type, status="error").inc()
            log_stage(logger, "event.publish_failed", "Failed to publish event",
                      level=logging.ERROR, error=str(exc), **ids)
            raise PublishError(str(exc)) from exc
        EVENTS_PUBLISHED.labels(type=event.type, status="success").inc()
        log_stage(logger, "event.published", "Event published",
                  stream=route.stream, messageId=message_id, **ids)
        return event, message_id


def make_handler(publisher: Publisher) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/metrics":
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            self._send(HTTPStatus.OK, generate_latest(), CONTENT_TYPE_LATEST)

        def do_POST(self) -> None:
            if self.path != "/jobs":
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return

            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY_BYTES:
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "body too large"})
                return
            body = self.rfile.read(length) if length else b"{}"
            try:
                request = JobRequest.model_validate_json(body)
            except ValidationError as exc:
                errors = [f"{'.'.join(map(str, e['loc'])) or 'body'}: {e['msg']}" for e in exc.errors()]
                log_stage(logger, "request.invalid", "Rejected job request",
                          level=logging.WARNING, errors=errors)
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid request", "details": errors})
                return

            try:
                event, message_id = publisher.publish(request)
            except PublishError:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "failed to publish event"})
                return
            self._send_json(HTTPStatus.ACCEPTED, {
                "jobId": event.jobId,
                "traceId": event.traceId,
                "messageId": message_id,
            })

        def _send_json(self, status: HTTPStatus, payload: dict) -> None:
            self._send(status, json.dumps(payload).encode(), "application/json")

        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            # Access logs are noise next to the stage logs (and /metrics is scraped every 15s).
            pass

    return Handler


def main() -> None:
    settings = ProducerSettings.from_env()
    setup_logging("producer", settings.log_level)

    for status in ("success", "error"):
        EVENTS_PUBLISHED.labels(type="http", status=status)
    for variant in (STABLE, PRERELEASE):
        EVENTS_ROUTED.labels(variant=variant)
    AB_MODE_ENABLED.set(int(settings.ab_mode_enabled))

    client = redis.Redis.from_url(
        settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=5
    )
    router = Router(client, settings.ab_mode_enabled, settings.stable_stream, settings.prerelease_stream)
    try:
        percent = router.read_percent()
    except redis.RedisError:
        percent = 0  # Redis not up yet; the percent is re-read on every request
    PRERELEASE_TRAFFIC_PERCENT.set(percent)
    log_stage(logger, "routing.config", "Routing configured", abModeEnabled=settings.ab_mode_enabled,
              stableStream=settings.stable_stream, prereleaseStream=settings.prerelease_stream,
              percentKey=PERCENT_KEY, percent=percent)

    server = ThreadingHTTPServer(("0.0.0.0", settings.http_port), make_handler(Publisher(client, router)))

    def shutdown(*_) -> None:
        # shutdown() blocks until serve_forever() returns, so call it off the main thread.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log_stage(logger, "producer.started", "Producer listening", port=settings.http_port)
    server.serve_forever()
    server.server_close()


if __name__ == "__main__":
    main()
