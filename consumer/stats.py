import redis

CREATED = "created"
SUCCEEDED = "succeeded"
FAILED = "failed"
STATUSES = (CREATED, SUCCEEDED, FAILED)

STATS_TTL_SECONDS = 7 * 24 * 3600

# KEYS[1] = jobs hash (jobId -> status), KEYS[2] = stats hash (status -> count)
# ARGV[1] = jobId, ARGV[2] = new status, ARGV[3] = TTL seconds
# A job is counted as created once, and as succeeded/failed once and only after created,
# so redelivered messages never double-count. Returns 1 if the status changed.
_TRANSITION = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
local allowed
if ARGV[2] == 'created' then
  allowed = not current
else
  allowed = current == 'created'
end
local changed = 0
if allowed then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
  changed = 1
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return changed
"""


class JobStats:
    """Per variant/version job counters persisted in Redis."""

    def __init__(self, client: redis.Redis, variant: str, version: str) -> None:
        self.client = client
        self.jobs_key = f"jobs:{variant}:{version}"
        self.stats_key = f"stats:{variant}:{version}"
        self._transition = client.register_script(_TRANSITION)

    def record(self, job_id: str, status: str) -> bool:
        changed = self._transition(keys=[self.jobs_key, self.stats_key], args=[job_id, status, STATS_TTL_SECONDS])
        return bool(changed)

    def counters(self) -> dict[str, int]:
        raw = self.client.hgetall(self.stats_key)
        return {status: int(raw.get(status, 0)) for status in STATUSES}
