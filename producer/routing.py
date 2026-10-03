import hashlib
from dataclasses import dataclass

import redis

PERCENT_KEY = "routing:prerelease_percent"

STABLE = "stable"
PRERELEASE = "prerelease"


@dataclass(frozen=True)
class Route:
    variant: str
    stream: str
    bucket: int
    percent: int


def bucket_for(job_id: str) -> int:
    """Deterministic bucket 0..99: the same jobId always lands in the same bucket."""
    return int(hashlib.sha256(job_id.encode()).hexdigest(), 16) % 100


def parse_percent(raw: str | None) -> int:
    """Missing or invalid values mean no pre-release traffic."""
    try:
        percent = int(raw)
    except (TypeError, ValueError):
        return 0
    return percent if 0 <= percent <= 100 else 0


class Router:
    def __init__(self, client: redis.Redis, enabled: bool, stable_stream: str, prerelease_stream: str) -> None:
        self.client = client
        self.enabled = enabled
        self.stable_stream = stable_stream
        self.prerelease_stream = prerelease_stream

    def read_percent(self) -> int:
        if not self.enabled:
            return 0
        return parse_percent(self.client.get(PERCENT_KEY))

    def route(self, job_id: str) -> Route:
        percent = self.read_percent()
        bucket = bucket_for(job_id)
        if bucket < percent:
            return Route(PRERELEASE, self.prerelease_stream, bucket, percent)
        return Route(STABLE, self.stable_stream, bucket, percent)
