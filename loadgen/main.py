import json
import logging
import signal
import threading
import urllib.request
import uuid

from shared.config import LoadgenSettings
from shared.logging import log_stage, setup_logging

logger = logging.getLogger("loadgen")


def post_job(url: str) -> None:
    body = json.dumps({"jobId": uuid.uuid4().hex[:12], "type": "http"}).encode()
    request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5):
        pass


def main() -> None:
    settings = LoadgenSettings.from_env()
    setup_logging("loadgen", settings.log_level)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    interval = 1 / settings.loadgen_rate_per_sec
    while not stop.wait(interval):
        try:
            post_job(settings.producer_url)
        except OSError as exc:
            log_stage(logger, "loadgen.request_failed", "Job request failed",
                      level=logging.ERROR, url=settings.producer_url, error=str(exc))


if __name__ == "__main__":
    main()
