import logging
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

import docker
import redis
from prometheus_client import start_http_server
from pydantic import ValidationError

import metrics
from dispatcher import Dispatcher
from handlers import JobFailed
from internal_api import start_status_server
from reaper import SandboxReaper, remove_owned_sandboxes
from runtime import DockerRuntime
from shared.config import ConsumerSettings
from shared.events import PAYLOAD_FIELD, JobEvent
from shared.logging import log_stage, setup_logging
from stats import CREATED, FAILED, SUCCEEDED, JobStats
from transport import RedisStreamTransport
from watcher import SandboxWatcher

logger = logging.getLogger("consumer")


class Consumer:
    def __init__(self, transport: RedisStreamTransport, dispatcher: Dispatcher, stats: JobStats) -> None:
        self.transport = transport
        self.dispatcher = dispatcher
        self.stats = stats

    def process(self, message_id: str, fields: dict[str, str]) -> None:
        try:
            event = JobEvent.model_validate_json(fields.get(PAYLOAD_FIELD) or "")
        except ValidationError as exc:
            errors = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'payload'}: {e['msg']}" for e in exc.errors())
            self._drop_invalid(message_id, f"invalid payload: {errors}")
            return

        ids = {"jobId": event.jobId, "traceId": event.traceId, "messageId": message_id}
        log_stage(logger, "event.received", "Event received", type=event.type, **ids)

        handler = self.dispatcher.get(event.type)
        if handler is None:
            self._drop_invalid(message_id, f"unknown job type {event.type!r}",
                               jobId=event.jobId, traceId=event.traceId)
            return

        self._record(event.jobId, CREATED)
        with metrics.labels(metrics.JOB_PROCESSING_DURATION).time():
            try:
                handler.handle(event)
            except JobFailed as exc:
                self._record(event.jobId, FAILED)
                metrics.labels(metrics.EVENTS_CONSUMED, type=event.type, result="failed").inc()
                log_stage(logger, "event.nacked", "Job failed; message left pending",
                          level=logging.WARNING, reason=exc.reason, **ids)
                return

        # Record before acking: if we crash in between, the redelivered message is not double-counted.
        self._record(event.jobId, SUCCEEDED)
        self.transport.ack(message_id)
        metrics.labels(metrics.EVENTS_CONSUMED, type=event.type, result="success").inc()
        log_stage(logger, "event.acked", "Event acked", **ids)

    def _record(self, job_id: str, status: str) -> None:
        try:
            self.stats.record(job_id, status)
        except redis.RedisError as exc:
            log_stage(logger, "stats.failed", "Could not record job status",
                      level=logging.ERROR, jobId=job_id, status=status, error=str(exc))

    def _drop_invalid(self, message_id: str, reason: str, **ids) -> None:
        # Retrying an invalid message can never succeed, so ack it to drop it.
        self.transport.ack(message_id)
        metrics.labels(metrics.EVENTS_CONSUMED, type="unknown", result="invalid").inc()
        log_stage(logger, "event.invalid", "Invalid event dropped",
                  level=logging.WARNING, reason=reason, messageId=message_id, **ids)


def sample_queue(transport: RedisStreamTransport, runtime: DockerRuntime, interval: float) -> None:
    while True:
        try:
            pending, oldest_age = transport.pending()
            metrics.labels(metrics.QUEUE_OLDEST_PENDING_AGE).set(oldest_age)
            metrics.labels(metrics.QUEUE_PENDING_MESSAGES, stream=transport.stream).set(pending)
            metrics.labels(metrics.QUEUE_STREAM_LENGTH, stream=transport.stream).set(transport.length())
            metrics.labels(metrics.SANDBOXES_ACTIVE).set(runtime.count_active())
        except Exception:
            logger.exception("Queue sampling failed", extra={"stage": "sampler.error"})
        time.sleep(interval)


def wait_for_group(transport: RedisStreamTransport, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            transport.ensure_group()
            return
        except redis.RedisError as exc:
            log_stage(logger, "redis.error", "Redis not ready; retrying",
                      level=logging.WARNING, error=str(exc))
            stop.wait(2)


def main() -> None:
    settings = ConsumerSettings.from_env()
    setup_logging("consumer", settings.log_level, version=settings.app_version, variant=settings.variant)

    metrics.init(settings.app_version, settings.variant, settings.max_sandboxes)
    start_http_server(settings.metrics_port)

    client = redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=settings.read_block_ms / 1000 + 5,
    )
    transport = RedisStreamTransport(
        client,
        stream=settings.stream_name,
        group=settings.consumer_group,
        consumer=settings.consumer_name,
        block_ms=settings.read_block_ms,
        count=settings.read_count,
    )
    runtime = DockerRuntime(settings)
    stats = JobStats(client, settings.variant, settings.app_version)
    consumer = Consumer(transport, Dispatcher(runtime, settings.failure_injection_percent), stats)

    instance_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    start_status_server(settings.internal_port, lambda: {
        "version": settings.app_version,
        "variant": settings.variant,
        "instanceId": instance_id,
        "startedAt": started_at,
        "jobs": stats.counters(),
        "sandboxesActive": runtime.count_active(),
    })

    stop = threading.Event()

    def request_stop(signum: int, _frame) -> None:
        log_stage(logger, "consumer.stopping", "Shutdown requested; finishing current work",
                  signal=signal.Signals(signum).name)
        stop.set()

    # SIGSTOP/SIGKILL cannot be caught; SIGTERM (docker stop) and SIGINT (Ctrl-C) can.
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        wait_for_group(transport, stop)
        SandboxWatcher(docker.from_env(), runtime).start()
        SandboxReaper(runtime, settings.sandbox_ttl_seconds, stop).start()
        threading.Thread(
            target=sample_queue,
            args=(transport, runtime, settings.queue_sample_interval_seconds),
            name="queue-sampler",
            daemon=True,
        ).start()

        log_stage(logger, "consumer.started", "Consumer started", stream=settings.stream_name,
                  group=settings.consumer_group, consumer=settings.consumer_name, instanceId=instance_id,
                  failureInjectionPercent=settings.failure_injection_percent)

        for message_id, fields in transport.own_pending():
            if stop.is_set():
                break
            consumer.process(message_id, fields)

        while not stop.is_set():
            try:
                for message_id, fields in transport.read_new():
                    if stop.is_set():
                        break  # the rest of the batch stays pending for the next start
                    consumer.process(message_id, fields)
            except redis.RedisError as exc:
                log_stage(logger, "redis.error", "Stream read failed; retrying",
                          level=logging.ERROR, error=str(exc))
                stop.wait(2)
                wait_for_group(transport, stop)
    finally:
        remove_owned_sandboxes(runtime)
        log_stage(logger, "consumer.stopped", "Consumer stopped")


if __name__ == "__main__":
    main()
