from handlers import JobHandler
from handlers.http import HttpJobHandler
from runtime import ContainerRuntime

HANDLERS: dict[str, type[JobHandler]] = {"http": HttpJobHandler}


class Dispatcher:
    def __init__(self, runtime: ContainerRuntime, failure_injection_percent: float = 0.0) -> None:
        self.handlers = {
            job_type: cls(runtime, failure_injection_percent) for job_type, cls in HANDLERS.items()
        }

    def get(self, job_type: str) -> JobHandler | None:
        return self.handlers.get(job_type)
