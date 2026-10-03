import json
import logging
import sys
from datetime import datetime, timezone

# Attributes every LogRecord has; anything else on the record came in via `extra`.
_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, static_fields: dict[str, str] | None = None) -> None:
        super().__init__()
        self.service = service
        self.static_fields = static_fields or {}

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "service": self.service,
            **self.static_fields,
            "stage": getattr(record, "stage", "log"),
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RECORD_ATTRS and key != "stage" and value is not None:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(service: str, level: str = "INFO", **static_fields: str) -> None:
    """`static_fields` (e.g. version, variant) are added to every line."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service, static_fields))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("urllib3", "docker"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_stage(
    logger: logging.Logger,
    stage: str,
    message: str,
    level: int = logging.INFO,
    exc_info: bool = False,
    **fields,
) -> None:
    """Log one JSON line for a pipeline stage; `fields` (jobId, traceId, url, ...) become top-level keys."""
    logger.log(level, message, extra={"stage": stage, **fields}, exc_info=exc_info)
