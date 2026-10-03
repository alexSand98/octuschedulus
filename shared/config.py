import os
from typing import Literal, Self

from pydantic import BaseModel, Field, field_validator


class EnvSettings(BaseModel):
    """Settings read from environment variables named after the upper-cased field names."""

    @classmethod
    def from_env(cls) -> Self:
        values = {
            name: os.environ[name.upper()]
            for name in cls.model_fields
            if name.upper() in os.environ
        }
        return cls(**values)


class CommonSettings(EnvSettings):
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379/0"


class ProducerSettings(CommonSettings):
    http_port: int = 8000
    ab_mode_enabled: bool = False
    stable_stream: str = "jobs:stable"
    prerelease_stream: str = "jobs:prerelease"


class ConsumerSettings(CommonSettings):
    stream_name: str = "jobs:stable"
    consumer_group: str = "workers-stable"
    consumer_name: str = "consumer-stable-1"
    app_version: str = "v1"
    variant: Literal["stable", "prerelease"] = "stable"
    read_block_ms: int = 5000
    read_count: int = 10
    metrics_port: int = 8001
    internal_port: int = 9000
    queue_sample_interval_seconds: float = 15.0
    sandbox_image: str = "python:3.12-slim"
    sandbox_public_host: str = "localhost"
    sandbox_network: str = "sandbox-net"
    sandbox_ready_timeout_seconds: float = 30.0
    sandbox_ttl_seconds: float = Field(60.0, gt=0)
    max_sandboxes: int = 20
    failure_injection_percent: float = Field(0.0, ge=0, le=100)


class ControllerSettings(CommonSettings):
    stable_status_url: str = "http://consumer-stable:9000/internal/status"
    prerelease_status_url: str = "http://consumer-prerelease:9000/internal/status"
    rollout_steps: tuple[int, ...] = (10, 25, 50, 100)
    eval_interval_seconds: float = 15.0
    min_hold_seconds: float = 60.0
    min_samples: int = 5
    step_timeout_seconds: float = 300.0
    min_success_ratio: float = 0.95
    max_success_diff: float = 0.02
    max_probe_failures: int = 4
    metrics_port: int = 8003

    @field_validator("rollout_steps", mode="before")
    @classmethod
    def _split_steps(cls, value):
        if isinstance(value, str):
            return tuple(int(step) for step in value.split(",") if step.strip())
        return value

    @field_validator("rollout_steps")
    @classmethod
    def _check_steps(cls, steps: tuple[int, ...]) -> tuple[int, ...]:
        if not steps or any(not 0 < step <= 100 for step in steps) or list(steps) != sorted(set(steps)):
            raise ValueError("ROLLOUT_STEPS must be strictly increasing percentages in 1..100")
        return steps


class LoadgenSettings(EnvSettings):
    log_level: str = "INFO"
    producer_url: str = "http://localhost:8000/jobs"
    loadgen_rate_per_sec: float = Field(0.5, gt=0)
