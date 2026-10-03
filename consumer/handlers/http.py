import logging
import random
import time
from datetime import datetime, timezone

import metrics
from handlers import JobFailed, JobHandler
from runtime import ContainerRuntime, SandboxStartError
from shared.events import JobEvent
from shared.logging import log_stage

logger = logging.getLogger("consumer.handlers.http")


class HttpJobHandler(JobHandler):
    def __init__(self, runtime: ContainerRuntime, failure_injection_percent: float = 0.0) -> None:
        self.runtime = runtime
        self.failure_injection_percent = failure_injection_percent

    def handle(self, event: JobEvent) -> None:
        ids = {"jobId": event.jobId, "traceId": event.traceId}
        log_stage(logger, "sandbox.starting", "Starting HTTP sandbox", **ids)

        if random.uniform(0, 100) < self.failure_injection_percent:
            self._fail("injected", f"failure injected for {self.failure_injection_percent}% of jobs", ids)

        started = time.perf_counter()
        try:
            url = self.runtime.start_http_sandbox(event.jobId)
        except SandboxStartError as exc:
            self._fail(exc.reason, str(exc), ids)
        except Exception as exc:
            self._fail("internal", repr(exc), ids, exc_info=True)

        duration = time.perf_counter() - started
        e2e_latency = (datetime.now(timezone.utc) - event.createdAt).total_seconds()
        metrics.labels(metrics.SANDBOX_START_DURATION).observe(duration)
        metrics.labels(metrics.JOB_E2E_LATENCY).observe(e2e_latency)
        log_stage(logger, "sandbox.ready", "Sandbox ready", url=url,
                  startDurationSeconds=round(duration, 3), e2eLatencySeconds=round(e2e_latency, 3), **ids)

    @staticmethod
    def _fail(reason: str, detail: str, ids: dict[str, str], exc_info: bool = False) -> None:
        metrics.labels(metrics.SANDBOX_FAILURES, reason=reason).inc()
        log_stage(logger, "sandbox.failed", "Sandbox failed to start",
                  level=logging.ERROR, exc_info=exc_info, reason=reason, error=detail, **ids)
        raise JobFailed(reason)
