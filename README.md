# Sandbox Platform

A small sandbox orchestration platform with A/B routing and automated rollout.

- The **producer** turns `POST /jobs` requests into `JobEvent`s on a Redis Stream.
  In A/B mode it picks either the stable or the pre-release stream for each job.
- Two **consumers** built from the same code run as `stable` (v1) and `prerelease` (v2-rc).
  Each starts one HTTP sandbox container per job (`python -m http.server`).
- The **rollout controller** watches both consumers. It moves pre-release traffic
  10 → 25 → 50 → 100 %, or aborts back to 0 %.
- **loadgen** sends a steady trickle of jobs, so a rollout always has data.
- **Prometheus** and **Grafana** show what is happening.

```
POST /jobs ─▶ producer ── A/B off ─────────────────────────▶ jobs:stable     ─▶ consumer-stable (v1)
                       └─ A/B on: hash(jobId)%100 < PCT ? ─▶ jobs:prerelease ─▶ consumer-prerelease (v2-rc)
                                                     else ─▶ jobs:stable
                              ▲ reads PCT
Redis: routing:prerelease_percent ◀── rollout-controller ──GET /internal/status──▶ both consumers
       rollout:state                                       (counters persisted in Redis)
       stats:{variant}:{version}, jobs:{variant}:{version} ◀── written by consumers
```

## Run

You need Docker with Compose v2. The consumers mount `/var/run/docker.sock`.

```bash
docker compose up --build
```

Nothing else is needed. loadgen starts posting jobs right away, and the controller
starts a rollout of `v2-rc`. A healthy rollout reaches `PROMOTED` in about 5–6 minutes.
Watch it with:

```bash
docker compose logs -f rollout-controller | grep -E 'rollout\.(started|step_advanced|promoted|gate_failed|aborted)'
```

or on the **Rollout** row of the Grafana dashboard (http://localhost:3000).

## How A/B routing works

- With `AB_MODE_ENABLED=false`, every job goes to `STABLE_STREAM`.
- With `AB_MODE_ENABLED=true`, the producer reads `routing:prerelease_percent` from Redis
  **on every request**. A missing or invalid value counts as 0. Then:
  ```
  bucket = int(sha256(jobId).hexdigest(), 16) % 100
  bucket < percent  → PRERELEASE_STREAM
  otherwise         → STABLE_STREAM
  ```
  Routing is deterministic: a jobId always lands in the same bucket.
- The producer forwards the event unchanged. The request payload, the `JobEvent` schema
  and the Redis message (one `payload` field holding the event JSON) are the same as
  before. Only the target stream changes.
- Each consumer takes its `VARIANT` and `APP_VERSION` from its environment, never from the
  message. It adds both to every log line, every metric (`version` and `variant` labels)
  and every sandbox container (`sandbox.version` and `sandbox.variant` labels).

## How the automated rollout works

Each consumer records every job in Redis using one atomic Lua script:

- `jobs:{variant}:{version}`: a hash of `jobId → created|succeeded|failed`
- `stats:{variant}:{version}`: a hash of counters `created`, `succeeded`, `failed`

A job counts as `created` once. It counts as `succeeded` or `failed` only while its
status is still `created`, so redelivered messages are never counted twice. Both keys
expire after 7 days, and the timer resets on every write.
`GET /internal/status` (port 9000, reachable only inside the Compose network) returns these
counters from Redis:

```json
{ "version": "v2-rc", "variant": "prerelease", "instanceId": "…", "startedAt": "…",
  "jobs": { "created": 12, "succeeded": 11, "failed": 1 }, "sandboxesActive": 3 }
```

Every `EVAL_INTERVAL_SECONDS`, the controller probes both endpoints and compares the
counters with the baselines it saved at the start of the current step:

```
                      pre-release version ≠ rollout:state.version
   (idle) ─────────────────────────────────────────────▶ PROGRESSING (step 0, percent = 10)
                                                              │
        ┌─────────────────────────────────────────────────────┤ every tick
        │                                                     ▼
        │  not enough signal (elapsed < MIN_HOLD or done < MIN_SAMPLES) ──▶ hold
        │      └─ elapsed > STEP_TIMEOUT ───────────────────────────────▶ ABORTED (percent 0)
        │  pre-release unreachable ×MAX_PROBE_FAILURES ─────────────────▶ ABORTED (percent 0)
        │  stable unreachable / error / missing data ───────────────────▶ hold (never advance)
        │  any gate fails ──────────────────────────────────────────────▶ ABORTED (percent 0)
        │  all gates pass, not the last step ──▶ next step (25 → 50 → 100), new baselines
        └──────────────────────────────────────┘
           all gates pass on the last step ─────────────────────────────▶ PROMOTED (percent 100)

   PROMOTED / ABORTED for the same version → idle until the pre-release version changes
```

The three gates are:

| Gate | Passes when |
|------|-------------|
| `success_ratio` | pre-release `Δsucceeded / (Δsucceeded + Δfailed) ≥ MIN_SUCCESS_RATIO` |
| `stable_comparison` | pre-release success ≥ stable success − `MAX_SUCCESS_DIFF` (only if stable finished ≥ 1 job in the step) |
| `stuck_jobs` | pre-release in-flight jobs (`Δcreated − done`) have not grown on 2 ticks in a row |

The routing percent always lives in `rollout:state`, and both are written in one
`MULTI`. All controller state is in Redis, so a restarted controller resumes the same
step. Redis runs with AOF (`--appendonly yes`) on the `redis-data` volume, so state and
counters survive a Redis restart too.

## Rollout operations

All `redis-cli` commands below run inside the Redis container, e.g.
`docker compose exec redis redis-cli HGETALL rollout:state`.

**Trigger a new rollout.** Change `APP_VERSION` of `consumer-prerelease`. Compose reads it
from `PRERELEASE_VERSION` (default `v2-rc`):

```bash
PRERELEASE_VERSION=v3-rc docker compose up -d consumer-prerelease
```

The controller notices the new version on its next tick and starts again at 10 %.
Keep the variable set, or edit `docker-compose.yml`, for later `docker compose up`
runs. Otherwise the version reverts to `v2-rc`, and that change counts as another
new version.

**Demo an abort.** Make the pre-release fail 30 % of its jobs (reason `injected`). The
current version has already finished its rollout, so either use a new version:

```bash
PRERELEASE_VERSION=v3-bad PRERELEASE_FAILURE_INJECTION_PERCENT=30 docker compose up -d consumer-prerelease
```

or keep the version and reset the rollout state first (see below). After about
1.5–2 minutes, the controller logs `rollout.gate_failed` (`gate=success_ratio`) and then
`rollout.aborted`, sets the percent to 0, and `RolloutAborted` fires in Prometheus.

**Inspect:**

```bash
docker compose exec redis redis-cli HGETALL rollout:state
docker compose exec redis redis-cli GET routing:prerelease_percent
docker compose exec redis redis-cli HGETALL stats:prerelease:v2-rc
docker compose exec redis redis-cli HGETALL stats:stable:v1
# The status endpoint is internal; call it from inside a container:
docker compose exec rollout-controller python -c \
  "import urllib.request; print(urllib.request.urlopen('http://consumer-prerelease:9000/internal/status').read().decode())"
```

**Reset** (the controller then starts a fresh rollout of the current pre-release version):

```bash
docker compose exec redis redis-cli DEL rollout:state routing:prerelease_percent
```

**Change the split by hand.** Stop the controller first (`docker compose stop rollout-controller`),
or it will overwrite your value:

```bash
docker compose exec redis redis-cli SET routing:prerelease_percent 25   # 0 = no pre-release traffic
```

With `AB_MODE_ENABLED=false` on the producer, all traffic goes to stable regardless of the key.

## Test it with curl

**1. Create a job.** Both body fields are optional. Without a `jobId`, the producer
generates one, and `type` defaults to `http`. The response does not say which variant
got the job. The producer's `event.routed` log line does.

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"jobId": "demo-1", "type": "http"}'
# 202 {"jobId": "demo-1", "traceId": "…", "messageId": "…"}

docker compose logs producer | grep '"event.routed"' | grep '"demo-1"'
```

**2. Find the sandbox URL** in the `sandbox.ready` line. Sandboxes are removed after
`SANDBOX_TTL_SECONDS` (60 s), so call it quickly:

```bash
URL=$(docker compose logs consumer-stable consumer-prerelease | grep '"sandbox.ready"' | grep '"demo-1"' \
      | tail -1 | sed -E 's/.*"url": "([^"]+)".*/\1/')
curl -s "$URL" | head          # directory listing
```

**3. Other checks:**

```bash
# 400: invalid jobId (only [a-zA-Z0-9_.-], max 63 chars) or unknown field
curl -s -X POST http://localhost:8000/jobs -d '{"jobId": "bad id!"}'

# Sandboxes per variant
docker ps --filter label=sandbox.managed=true \
  --format '{{.Names}}\t{{.Label "sandbox.variant"}}\t{{.Label "sandbox.version"}}\t{{.Ports}}'

# Metrics
curl -s http://localhost:8000/metrics | grep -E 'events_routed_total|prerelease_traffic_percent'
curl -s http://localhost:8002/metrics | grep events_consumed_total
curl -s http://localhost:8003/metrics | grep -E '^rollout_'
```

### Producer API

| Method | Path       | Description |
|--------|------------|-------------|
| POST   | `/jobs`    | Body `{"jobId"?: str, "type"?: str}` → `202 {jobId, traceId, messageId}`. Returns `400` for an invalid body and `503` if Redis is unavailable. |
| GET    | `/metrics` | Prometheus metrics |

## UIs and ports

| Service                       | URL                            | Notes |
|-------------------------------|--------------------------------|-------|
| Grafana                       | http://localhost:3000          | admin / admin (anonymous view enabled). Dashboard: **Sandbox Platform Overview** |
| Prometheus                    | http://localhost:9090          | Alerts: http://localhost:9090/alerts (no Alertmanager in this stack) |
| Producer API                  | http://localhost:8000          | `POST /jobs`, `GET /metrics` |
| consumer-stable metrics       | http://localhost:8001/metrics  | |
| consumer-prerelease metrics   | http://localhost:8002/metrics  | |
| rollout-controller metrics    | http://localhost:8003/metrics  | |
| Redis                         | localhost:6379                 | |
| Consumer `/internal/status`   | port 9000                      | Inside the Compose network only, not published |
| Sandboxes                     | http://localhost:<random port> | Shown in the `sandbox.ready` log line |

Logs are JSON on stdout. Use `docker compose logs <service>` and filter with `grep`/`jq`, e.g.
`docker compose logs consumer-prerelease | grep '"jobId": "demo-1"'`.

## Environment variables

### Producer

| Variable            | Default                    | Description |
|---------------------|----------------------------|-------------|
| `REDIS_URL`         | `redis://localhost:6379/0` | Redis connection URL |
| `HTTP_PORT`         | `8000`                     | Port for the API and `/metrics` |
| `AB_MODE_ENABLED`   | `false`                    | Route a share of jobs to the pre-release stream (compose: `true`) |
| `STABLE_STREAM`     | `jobs:stable`              | Stream for stable traffic |
| `PRERELEASE_STREAM` | `jobs:prerelease`          | Stream for pre-release traffic |
| `LOG_LEVEL`         | `INFO`                     | Log level |

The pre-release share itself is not an env var. It is the Redis key
`routing:prerelease_percent` (0–100), which the rollout controller owns.

### Consumer

| Variable                        | Default                    | Description |
|---------------------------------|----------------------------|-------------|
| `REDIS_URL`                     | `redis://localhost:6379/0` | Redis connection URL |
| `STREAM_NAME`                   | `jobs:stable`              | Stream to consume |
| `CONSUMER_GROUP`                | `workers-stable`           | Consumer group (created with MKSTREAM if missing) |
| `CONSUMER_NAME`                 | `consumer-stable-1`        | Name in the group, also the `sandbox.owner` label. Keep it stable so pending messages are reclaimed on restart |
| `APP_VERSION`                   | `v1`                       | Version reported in logs, metrics, labels and `/internal/status` |
| `VARIANT`                       | `stable`                   | `stable` or `prerelease` |
| `INTERNAL_PORT`                 | `9000`                     | Port for `GET /internal/status` |
| `METRICS_PORT`                  | `8001`                     | Port for `/metrics` |
| `READ_BLOCK_MS`                 | `5000`                     | XREADGROUP block timeout |
| `READ_COUNT`                    | `10`                       | Max messages per read |
| `QUEUE_SAMPLE_INTERVAL_SECONDS` | `15`                       | How often stream length, pending count and oldest pending age are sampled |
| `SANDBOX_IMAGE`                 | `python:3.12-slim`         | Sandbox image |
| `SANDBOX_PUBLIC_HOST`           | `localhost`                | Host used in the returned sandbox URL |
| `SANDBOX_NETWORK`               | `sandbox-net`              | Docker network for sandboxes. It must be shared with the consumer, which probes `http://sandbox-<jobId>:8080/` before `sandbox.ready` |
| `SANDBOX_READY_TIMEOUT_SECONDS` | `30`                       | How long to wait for a sandbox to answer (`reason=timeout` after that) |
| `SANDBOX_TTL_SECONDS`           | `60`                       | Sandboxes of this variant older than this are removed (`sandbox.removed`, `reason=ttl`) |
| `MAX_SANDBOXES`                 | `20` (compose: `50`)       | Max running sandboxes **per variant** (`reason=capacity`) |
| `FAILURE_INJECTION_PERCENT`     | `0`                        | Randomly fail this share of jobs with `reason=injected` (for demoing an abort) |
| `LOG_LEVEL`                     | `INFO`                     | Log level |

Compose also reads `PRERELEASE_VERSION` (default `v2-rc`) and
`PRERELEASE_FAILURE_INJECTION_PERCENT` (default `0`) from your shell. It passes them to
`consumer-prerelease` as `APP_VERSION` and `FAILURE_INJECTION_PERCENT`.

### Rollout controller

| Variable                | Default                                          | Description |
|-------------------------|--------------------------------------------------|-------------|
| `REDIS_URL`             | `redis://localhost:6379/0`                       | Redis connection URL |
| `STABLE_STATUS_URL`     | `http://consumer-stable:9000/internal/status`    | Stable status endpoint |
| `PRERELEASE_STATUS_URL` | `http://consumer-prerelease:9000/internal/status`| Pre-release status endpoint |
| `ROLLOUT_STEPS`         | `10,25,50,100`                                   | Traffic percent per step (strictly increasing) |
| `EVAL_INTERVAL_SECONDS` | `15`                                             | Time between ticks |
| `MIN_HOLD_SECONDS`      | `60`                                             | Minimum time on a step before gates are evaluated |
| `MIN_SAMPLES`           | `5`                                              | Minimum finished pre-release jobs per step |
| `STEP_TIMEOUT_SECONDS`  | `300`                                            | Abort if a step lacks enough signal for this long. Update `RolloutStuck` in `alerts.yml` if you change it |
| `MIN_SUCCESS_RATIO`     | `0.95`                                           | `success_ratio` gate |
| `MAX_SUCCESS_DIFF`      | `0.02`                                           | Allowed success gap vs stable |
| `MAX_PROBE_FAILURES`    | `4`                                              | Consecutive failed pre-release probes before abort |
| `METRICS_PORT`          | `8003`                                           | Port for `/metrics` |
| `LOG_LEVEL`             | `INFO`                                           | Log level |

### Load generator

| Variable               | Default                      | Description |
|------------------------|------------------------------|-------------|
| `PRODUCER_URL`         | `http://localhost:8000/jobs` | Where to POST jobs (compose: `http://producer:8000/jobs`) |
| `LOADGEN_RATE_PER_SEC` | `0.5`                        | Jobs per second |
| `LOG_LEVEL`            | `INFO`                       | Log level (only errors are logged) |

## Redis keys

| Key | Type | Written by | Content |
|-----|------|------------|---------|
| `jobs:stable`, `jobs:prerelease` | stream | producer | Job events (`payload` field) |
| `routing:prerelease_percent` | string | controller | Pre-release share 0–100 |
| `rollout:state` | hash | controller | `version`, `status`, `stepIndex`, `percent`, `stepStartedAt`, step baselines (`pre_*`, `stable_*`), `reason`, plus probe-failure and stuck-job tracking |
| `stats:{variant}:{version}` | hash | consumers | `created`, `succeeded`, `failed` |
| `jobs:{variant}:{version}` | hash | consumers | `jobId → created\|succeeded\|failed` |

## Alerts

`ServiceDown`, `PublishErrors`, `QueueBacklog`, `SandboxFailures`, `SlowSandboxStart` and
`CapacityHigh` are evaluated per variant where that makes sense. Two rollout alerts are added:
`RolloutAborted` (critical) and `RolloutStuck` (warning: progressing with the percent
unchanged for more than `STEP_TIMEOUT_SECONDS` + 2m). This stack has no Alertmanager,
redis_exporter, cAdvisor or Loki. Stream length and pending counts come from the
consumers' own metrics.

## Behaviour notes

- **No retries.** A failed job is logged as `event.nacked` and left pending. It is only
  processed again after a consumer restart, which reclaims the consumer's pending
  messages. In stats it stays `failed`.
- **Invalid payloads and unknown types** are logged as `event.invalid`, then acked and
  dropped. They are not counted in stats.
- **Promotion** means 100 % of traffic goes to the pre-release consumer. Promotion does not
  redeploy stable. The next new pre-release version starts again at 10 %.
- **Shutdown cleanup.** On SIGTERM (`docker stop`, `docker compose down`) or SIGINT, a
  consumer finishes its current job, stops reading, and removes every sandbox labelled
  with its `sandbox.owner`. SIGKILL and SIGSTOP cannot be caught and leave sandboxes behind.

## Cleanup

```bash
docker compose down            # consumers remove their sandboxes on SIGTERM
docker compose down -v         # also drops the Redis volume (rollout state, stats, streams)
docker rm -f $(docker ps -aq --filter label=sandbox.managed=true)   # only after a SIGKILL
```

## Layout

```
shared/              JobEvent schema, env settings, JSON logging (pip-installed into every image)
producer/            HTTP API → routing.py (A/B stream choice) → XADD, metrics
consumer/            transport, dispatcher, handlers/http, runtime (Docker), watcher, reaper (TTL),
                     stats (Lua counters), internal_api (/internal/status), metrics
rollout-controller/  controller.py (state machine, gates, Redis state), main.py (probe loop)
loadgen/             steady POST /jobs traffic
observability/       prometheus (+alerts), grafana provisioning + dashboard
```

`observability/alertmanager/` and `observability/alloy/` are no longer used by
`docker-compose.yml`. They are kept in case those services come back.
