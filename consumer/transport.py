import time
from collections.abc import Iterator

import redis

Message = tuple[str, dict[str, str]]


class RedisStreamTransport:
    """Consumer-group access to one Redis Stream."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        group: str,
        consumer: str,
        block_ms: int,
        count: int,
    ) -> None:
        self.client = client
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.block_ms = block_ms
        self.count = count

    def ensure_group(self) -> None:
        try:
            self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def own_pending(self) -> Iterator[Message]:
        """Messages already delivered to this consumer but never acked (e.g. before a restart)."""
        last_id = "0"
        while True:
            entries = self._read(last_id, block_ms=None)
            if not entries:
                return
            for message_id, fields in entries:
                last_id = message_id
                yield message_id, fields

    def read_new(self) -> list[Message]:
        return self._read(">", block_ms=self.block_ms)

    def ack(self, message_id: str) -> None:
        self.client.xack(self.stream, self.group, message_id)

    def length(self) -> int:
        return self.client.xlen(self.stream)

    def pending(self) -> tuple[int, float]:
        """Number of pending messages in the group and the age in seconds of the oldest one."""
        summary = self.client.xpending(self.stream, self.group)
        oldest_id = summary.get("min")
        if not oldest_id:
            return summary.get("pending", 0), 0.0
        # Stream entry IDs are "<unix-ms>-<seq>".
        created_ms = int(oldest_id.split("-", 1)[0])
        return summary["pending"], max(0.0, time.time() - created_ms / 1000)

    def _read(self, start_id: str, block_ms: int | None) -> list[Message]:
        response = self.client.xreadgroup(
            self.group, self.consumer, {self.stream: start_id}, count=self.count, block=block_ms
        )
        if not response:
            return []
        _, entries = response[0]
        # Entries deleted from the stream while pending come back with no fields.
        return [(message_id, fields or {}) for message_id, fields in entries]
