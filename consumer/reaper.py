import logging
import threading

from docker.errors import DockerException

from runtime import ContainerRuntime
from shared.logging import log_stage

logger = logging.getLogger("consumer.reaper")

REAP_INTERVAL_SECONDS = 5.0


def remove_sandboxes(runtime: ContainerRuntime, job_ids: list[str], reason: str) -> None:
    for job_id in job_ids:
        try:
            runtime.remove_sandbox(job_id)
        except DockerException as exc:
            log_stage(logger, "sandbox.remove_failed", "Failed to remove sandbox",
                      level=logging.ERROR, jobId=job_id, reason=reason, error=str(exc))
        else:
            log_stage(logger, "sandbox.removed", "Sandbox removed", jobId=job_id, reason=reason)


def remove_owned_sandboxes(runtime: ContainerRuntime) -> None:
    """Stop and delete every sandbox this consumer started (used on shutdown)."""
    try:
        job_ids = runtime.owned_job_ids()
    except DockerException as exc:
        log_stage(logger, "sandbox.cleanup_failed", "Could not list sandboxes to remove",
                  level=logging.ERROR, error=str(exc))
        return
    remove_sandboxes(runtime, job_ids, reason="shutdown")


class SandboxReaper(threading.Thread):
    """Removes this variant's sandboxes once they are older than the TTL, so load cannot exhaust capacity."""

    def __init__(self, runtime: ContainerRuntime, ttl_seconds: float, stop: threading.Event) -> None:
        super().__init__(name="sandbox-reaper", daemon=True)
        self.runtime = runtime
        self.ttl_seconds = ttl_seconds
        self.stop = stop

    def run(self) -> None:
        while not self.stop.wait(REAP_INTERVAL_SECONDS):
            try:
                expired = self.runtime.expired_job_ids(self.ttl_seconds)
            except DockerException as exc:
                log_stage(logger, "reaper.error", "Could not list expired sandboxes",
                          level=logging.ERROR, error=str(exc))
                continue
            remove_sandboxes(self.runtime, expired, reason="ttl")
