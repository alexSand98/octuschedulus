import logging
import threading
import time

import docker

import metrics
from runtime import LABEL_JOB_ID, DockerRuntime
from shared.logging import log_stage

logger = logging.getLogger("consumer.watcher")

# Container events after which the number of running sandboxes may have changed.
STATE_ACTIONS = {"start", "die", "destroy"}


def exit_reason(exit_code: str | None) -> str:
    if exit_code == "0":
        return "completed"
    if exit_code == "137":
        return "killed"
    return "error"


class SandboxWatcher(threading.Thread):
    """Follows Docker events for this variant's sandboxes and keeps lifecycle metrics in sync."""

    def __init__(self, client: docker.DockerClient, runtime: DockerRuntime) -> None:
        super().__init__(name="sandbox-watcher", daemon=True)
        self.client = client
        self.runtime = runtime

    def run(self) -> None:
        while True:
            try:
                self.sync_active()
                events = self.client.events(
                    decode=True, filters={"type": "container", "label": self.runtime.variant_filters}
                )
                for event in events:
                    self.on_event(event)
            except Exception:
                logger.exception("Docker event stream failed; reconnecting",
                                 extra={"stage": "watcher.error"})
                time.sleep(5)

    def on_event(self, event: dict) -> None:
        action = event.get("Action", "")
        attributes = event.get("Actor", {}).get("Attributes", {})
        fields = {"jobId": attributes.get(LABEL_JOB_ID), "container": attributes.get("name")}

        if action == "oom":
            metrics.labels(metrics.SANDBOX_EXITS, reason="oom").inc()
            log_stage(logger, "sandbox.oom", "Sandbox ran out of memory", level=logging.ERROR, **fields)
        elif action == "die":
            exit_code = attributes.get("exitCode")
            reason = exit_reason(exit_code)
            metrics.labels(metrics.SANDBOX_EXITS, reason=reason).inc()
            log_stage(logger, "sandbox.exited", "Sandbox exited", level=logging.WARNING,
                      reason=reason, exitCode=exit_code, **fields)

        if action in STATE_ACTIONS:
            self.sync_active()

    def sync_active(self) -> None:
        metrics.labels(metrics.SANDBOXES_ACTIVE).set(self.runtime.count_active())
