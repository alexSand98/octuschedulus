import json
import logging
import signal
import threading
import time
import urllib.request

import redis
from prometheus_client import start_http_server

from controller import RolloutController, Probe, StateStore
from shared.config import ControllerSettings
from shared.logging import log_stage, setup_logging

logger = logging.getLogger("rollout-controller")

PROBE_TIMEOUT_SECONDS = 3


def probe(name: str, url: str) -> Probe | None:
    """Read a consumer's /internal/status; None if unreachable or malformed."""
    try:
        with urllib.request.urlopen(url, timeout=PROBE_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
        jobs = payload["jobs"]
        return Probe(
            version=str(payload["version"]),
            created=int(jobs["created"]),
            succeeded=int(jobs["succeeded"]),
            failed=int(jobs["failed"]),
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        log_stage(logger, "probe.failed", "Status probe failed", level=logging.WARNING,
                  target=name, url=url, error=str(exc))
        return None


def main() -> None:
    settings = ControllerSettings.from_env()
    setup_logging("rollout-controller", settings.log_level)
    start_http_server(settings.metrics_port)

    client = redis.Redis.from_url(
        settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=5
    )
    controller = RolloutController(StateStore(client), settings)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    log_stage(logger, "controller.started", "Rollout controller started",
              steps=list(settings.rollout_steps), evalIntervalSeconds=settings.eval_interval_seconds,
              minHoldSeconds=settings.min_hold_seconds, minSamples=settings.min_samples,
              stepTimeoutSeconds=settings.step_timeout_seconds, minSuccessRatio=settings.min_success_ratio,
              maxSuccessDiff=settings.max_success_diff, maxProbeFailures=settings.max_probe_failures)

    while not stop.is_set():
        pre = probe("prerelease", settings.prerelease_status_url)
        stable = probe("stable", settings.stable_status_url)
        try:
            controller.tick(pre, stable, time.time())
        except Exception as exc:
            # Fail-safe: on any error (Redis down, corrupt state, ...) nothing is written, so we hold.
            log_stage(logger, "rollout.hold", "Holding rollout after an error", level=logging.ERROR,
                      holdReason="error", error=repr(exc))
        stop.wait(settings.eval_interval_seconds)


if __name__ == "__main__":
    main()
