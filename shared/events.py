from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

# The job id becomes part of a container name, so keep it to Docker-safe characters.
JOB_ID_PATTERN = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$"

PAYLOAD_FIELD = "payload"


class JobEvent(BaseModel):
    jobId: str = Field(pattern=JOB_ID_PATTERN)
    type: str
    createdAt: datetime
    traceId: str

    @field_validator("createdAt")
    @classmethod
    def _as_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_fields(self) -> dict[str, str]:
        """Redis Stream fields for XADD."""
        return {PAYLOAD_FIELD: self.model_dump_json()}
