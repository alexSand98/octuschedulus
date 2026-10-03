import time
import urllib.request
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import docker
from docker.errors import APIError, ImageNotFound, NotFound
from docker.models.containers import Container

from shared.config import ConsumerSettings

LABEL_JOB_ID = "sandbox.job_id"
LABEL_TYPE = "sandbox.type"
LABEL_MANAGED = "sandbox.managed"
LABEL_OWNER = "sandbox.owner"
LABEL_VERSION = "sandbox.version"
LABEL_VARIANT = "sandbox.variant"
MANAGED_FILTER = f"{LABEL_MANAGED}=true"

SANDBOX_PORT = 8080


class SandboxStartError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason


class ContainerRuntime(ABC):
    @abstractmethod
    def start_http_sandbox(self, job_id: str) -> str:
        """Start (or reuse) the HTTP sandbox for a job and return its public URL."""

    @abstractmethod
    def count_active(self) -> int:
        """Number of running sandboxes of this consumer's variant."""

    @abstractmethod
    def owned_job_ids(self) -> list[str]:
        """Job ids of all sandboxes (running or not) started by this consumer."""

    @abstractmethod
    def expired_job_ids(self, max_age_seconds: float) -> list[str]:
        """Job ids of this variant's sandboxes created more than `max_age_seconds` ago."""

    @abstractmethod
    def remove_sandbox(self, job_id: str) -> None:
        """Stop and delete the sandbox of a job; no-op if it does not exist."""


class DockerRuntime(ContainerRuntime):
    def __init__(self, settings: ConsumerSettings, client: docker.DockerClient | None = None) -> None:
        self.client = client or docker.from_env()
        self.image = settings.sandbox_image
        self.public_host = settings.sandbox_public_host
        self.network = settings.sandbox_network
        self.ready_timeout = settings.sandbox_ready_timeout_seconds
        self.max_sandboxes = settings.max_sandboxes
        self.owner = settings.consumer_name
        self.version = settings.app_version
        self.variant = settings.variant
        # Docker ANDs multiple label filters: managed sandboxes of this variant only.
        self.variant_filters = [MANAGED_FILTER, f"{LABEL_VARIANT}={self.variant}"]

    def count_active(self) -> int:
        return len(self._list(filters={"label": self.variant_filters, "status": "running"}))

    def owned_job_ids(self) -> list[str]:
        containers = self._list(all=True, filters={"label": [MANAGED_FILTER, f"{LABEL_OWNER}={self.owner}"]})
        return [container.labels[LABEL_JOB_ID] for container in containers]

    def expired_job_ids(self, max_age_seconds: float) -> list[str]:
        now = datetime.now(timezone.utc)
        containers = self._list(all=True, filters={"label": self.variant_filters})
        return [
            container.labels[LABEL_JOB_ID]
            for container in containers
            if (now - self._created_at(container)).total_seconds() > max_age_seconds
        ]

    def remove_sandbox(self, job_id: str) -> None:
        container = self._find(self._name(job_id))
        if container is not None:
            # force=True kills a running container before deleting it.
            container.remove(force=True)

    def start_http_sandbox(self, job_id: str) -> str:
        name = self._name(job_id)
        container = self._find(name)
        if container is not None and container.status != "running":
            # Leftover from an earlier attempt that never became ready; replace it.
            container.remove(force=True)
            container = None

        if container is None:
            if self.count_active() >= self.max_sandboxes:
                raise SandboxStartError("capacity", f"{self.max_sandboxes} sandboxes already running")
            container = self._create(name, job_id)

        port = self._wait_ready(container)
        return f"http://{self.public_host}:{port}"

    @staticmethod
    def _name(job_id: str) -> str:
        return f"sandbox-{job_id}"

    @staticmethod
    def _created_at(container: Container) -> datetime:
        # "Created" looks like 2026-10-03T10:36:30.123456789Z; second precision is enough.
        return datetime.strptime(container.attrs["Created"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    def _list(self, **kwargs) -> list[Container]:
        # Sandboxes can be removed concurrently (TTL reaper); skip ones that vanish while listing.
        return self.client.containers.list(ignore_removed=True, **kwargs)

    def _find(self, name: str) -> Container | None:
        try:
            return self.client.containers.get(name)
        except NotFound:
            return None

    def _create(self, name: str, job_id: str) -> Container:
        try:
            return self.client.containers.run(
                self.image,
                command=["python", "-m", "http.server", str(SANDBOX_PORT)],
                name=name,
                labels={
                    LABEL_JOB_ID: job_id,
                    LABEL_TYPE: "http",
                    LABEL_MANAGED: "true",
                    LABEL_OWNER: self.owner,
                    LABEL_VERSION: self.version,
                    LABEL_VARIANT: self.variant,
                },
                ports={f"{SANDBOX_PORT}/tcp": None},
                network=self.network,
                detach=True,
            )
        except ImageNotFound as exc:
            raise SandboxStartError("image_not_found", str(exc)) from exc
        except APIError as exc:
            raise SandboxStartError("docker_api", str(exc)) from exc

    def _wait_ready(self, container: Container) -> str:
        """Wait for the host port to be assigned and the server to answer; return the host port."""
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            container.reload()
            if container.status in ("exited", "dead"):
                raise SandboxStartError("exited", f"container {container.name} is {container.status}")
            bindings = (container.ports or {}).get(f"{SANDBOX_PORT}/tcp")
            if bindings and self._responds(container.name):
                return bindings[0]["HostPort"]
            time.sleep(0.5)
        raise SandboxStartError("timeout", f"container {container.name} not ready in {self.ready_timeout}s")

    @staticmethod
    def _responds(name: str) -> bool:
        # The consumer shares SANDBOX_NETWORK with the sandbox, so its name resolves via Docker DNS.
        try:
            with urllib.request.urlopen(f"http://{name}:{SANDBOX_PORT}/", timeout=1):
                return True
        except OSError:
            return False
