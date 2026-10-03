from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase

DURATION_BUCKETS = (0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120)

# Every consumer metric carries the instance's version and variant (set once in init()).
BASE = ("version", "variant")

EVENTS_CONSUMED = Counter(
    "events_consumed_total", "Job events consumed from the stream", [*BASE, "type", "result"]
)
JOB_PROCESSING_DURATION = Histogram(
    "job_processing_duration_seconds", "Time spent handling one job event", BASE, buckets=DURATION_BUCKETS
)
SANDBOX_START_DURATION = Histogram(
    "sandbox_start_duration_seconds", "Time from sandbox start request to ready", BASE, buckets=DURATION_BUCKETS
)
SANDBOXES_ACTIVE = Gauge("sandboxes_active", "Running sandbox containers of this variant", BASE)
SANDBOXES_MAX = Gauge("sandboxes_max", "Maximum allowed running sandboxes per variant (MAX_SANDBOXES)", BASE)
SANDBOX_FAILURES = Counter("sandbox_failures_total", "Sandbox start failures", [*BASE, "reason"])
SANDBOX_EXITS = Counter("sandbox_exits_total", "Sandbox container exits", [*BASE, "reason"])
JOB_E2E_LATENCY = Histogram(
    "job_e2e_latency_seconds", "Time from event creation to sandbox ready", BASE, buckets=DURATION_BUCKETS
)
QUEUE_OLDEST_PENDING_AGE = Gauge(
    "queue_oldest_pending_age_seconds", "Age of the oldest pending (unacked) message in the group", BASE
)
QUEUE_STREAM_LENGTH = Gauge("queue_stream_length", "Entries in the consumed stream", [*BASE, "stream"])
QUEUE_PENDING_MESSAGES = Gauge(
    "queue_pending_messages", "Pending (delivered, unacked) messages in the group", [*BASE, "stream"]
)

FAILURE_REASONS = ("capacity", "image_not_found", "docker_api", "exited", "timeout", "injected", "internal")
EXIT_REASONS = ("completed", "error", "killed", "oom")

_base_labels: dict[str, str] = {}


def labels(metric: MetricWrapperBase, **extra: str):
    """The metric child for this instance's version/variant plus any extra labels."""
    return metric.labels(**_base_labels, **extra)


def init(version: str, variant: str, max_sandboxes: int) -> None:
    """Set static values and pre-create known label sets so panels show 0 instead of no data."""
    _base_labels.update(version=version, variant=variant)
    labels(SANDBOXES_MAX).set(max_sandboxes)
    for result in ("success", "failed"):
        labels(EVENTS_CONSUMED, type="http", result=result)
    labels(EVENTS_CONSUMED, type="unknown", result="invalid")
    for reason in FAILURE_REASONS:
        labels(SANDBOX_FAILURES, reason=reason)
    for reason in EXIT_REASONS:
        labels(SANDBOX_EXITS, reason=reason)
