from abc import ABC, abstractmethod

from shared.events import JobEvent


class JobFailed(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class JobHandler(ABC):
    @abstractmethod
    def handle(self, event: JobEvent) -> None:
        """Process the job; raise JobFailed if it did not succeed."""
