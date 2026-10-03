import logging
from dataclasses import dataclass

import redis
from prometheus_client import Counter, Gauge
from pydantic import BaseModel

from shared.config import ControllerSettings
from shared.logging import log_stage

logger = logging.getLogger("rollout-controller")

STATE_KEY = "rollout:state"
PERCENT_KEY = "routing:prerelease_percent"

PROGRESSING = "PROGRESSING"
PROMOTED = "PROMOTED"
ABORTED = "ABORTED"
STATUS_CODES = {"": 0, PROGRESSING: 1, PROMOTED: 2, ABORTED: 3}

GATES = ("success_ratio", "stable_comparison", "stuck_jobs")
DECISIONS = ("start", "hold", "advance", "promote", "abort", "idle")
STUCK_TICKS = 2

ROLLOUT_STATUS = Gauge("rollout_status", "0 idle, 1 progressing, 2 promoted, 3 aborted")
ROLLOUT_PERCENT = Gauge("rollout_percent", "Pre-release traffic percent set by the controller")
ROLLOUT_GATE_PASSED = Gauge("rollout_gate_passed", "1 if the gate passed at the last evaluation", ["gate"])
ROLLOUT_DECISIONS = Counter("rollout_decisions_total", "Controller decisions per tick", ["decision"])
PRERELEASE_SUCCESS_RATIO = Gauge("rollout_prerelease_success_ratio", "Pre-release success ratio in the current step")
STABLE_SUCCESS_RATIO = Gauge("rollout_stable_success_ratio", "Stable success ratio in the current step")


@dataclass(frozen=True)
class Probe:
    """What the controller needs from a consumer's /internal/status."""

    version: str
    created: int
    succeeded: int
    failed: int


class RolloutState(BaseModel):
    """Mirror of the `rollout:state` hash; everything needed to resume after a restart."""

    version: str = ""
    status: str = ""
    stepIndex: int = 0
    percent: int = 0
    stepStartedAt: float = 0.0
    # Counters at the start of the current step.
    pre_succeeded: int = 0
    pre_failed: int = 0
    pre_created: int = 0
    stable_succeeded: int = 0
    stable_failed: int = 0
    reason: str = ""
    # Consecutive failed pre-release probes, and stuck-job tracking for the current step.
    probe_failures: int = 0
    inflight_last: int = 0
    inflight_growth_ticks: int = 0


class StateStore:
    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    def load(self) -> RolloutState:
        return RolloutState.model_validate(self.client.hgetall(STATE_KEY))

    def save(self, state: RolloutState) -> None:
        # The routing percent always changes together with the state.
        pipe = self.client.pipeline(transaction=True)
        pipe.hset(STATE_KEY, mapping={key: str(value) for key, value in state.model_dump().items()})
        pipe.set(PERCENT_KEY, state.percent)
        pipe.execute()


class RolloutController:
    def __init__(self, store: StateStore, settings: ControllerSettings) -> None:
        self.store = store
        self.settings = settings
        self.steps = settings.rollout_steps
        for decision in DECISIONS:
            ROLLOUT_DECISIONS.labels(decision=decision)

    def tick(self, pre: Probe | None, stable: Probe | None, now: float) -> str:
        state = self.store.load()
        decision = self._decide(state, pre, stable, now)
        ROLLOUT_DECISIONS.labels(decision=decision).inc()
        ROLLOUT_STATUS.set(STATUS_CODES.get(state.status, 0))
        ROLLOUT_PERCENT.set(state.percent)
        return decision

    def _decide(self, state: RolloutState, pre: Probe | None, stable: Probe | None, now: float) -> str:
        if pre is None:
            if state.status != PROGRESSING:
                return self._hold(state, "prerelease_unreachable", save=False)
            state.probe_failures += 1
            if state.probe_failures >= self.settings.max_probe_failures:
                return self._abort(state, "prerelease_unreachable", probeFailures=state.probe_failures)
            return self._hold(state, "prerelease_unreachable", probeFailures=state.probe_failures)
        state.probe_failures = 0

        if stable is None:
            # Never abort because of stable; just wait for it.
            return self._hold(state, "stable_unreachable", save=state.status == PROGRESSING)

        if pre.version != state.version:
            return self._start(state, pre, stable, now)
        if state.status != PROGRESSING:
            return "idle"
        return self._evaluate(state, pre, stable, now)

    def _start(self, state: RolloutState, pre: Probe, stable: Probe, now: float) -> str:
        previous = state.version
        state.version = pre.version
        state.status = PROGRESSING
        state.stepIndex = 0
        state.percent = self.steps[0]
        state.reason = ""
        self._begin_step(state, pre, stable, now)
        self.store.save(state)
        log_stage(logger, "rollout.started", "Rollout started", previousVersion=previous or None,
                  **self._fields(state))
        return "start"

    def _evaluate(self, state: RolloutState, pre: Probe, stable: Probe, now: float) -> str:
        pre_succeeded = pre.succeeded - state.pre_succeeded
        pre_failed = pre.failed - state.pre_failed
        pre_created = pre.created - state.pre_created
        stable_succeeded = stable.succeeded - state.stable_succeeded
        stable_failed = stable.failed - state.stable_failed
        if min(pre_succeeded, pre_failed, pre_created, stable_succeeded, stable_failed) < 0:
            # Counters went backwards (keys deleted/expired): data cannot be trusted, never advance.
            return self._hold(state, "counters_reset")

        pre_done = pre_succeeded + pre_failed
        stable_done = stable_succeeded + stable_failed
        inflight = pre_created - pre_done
        state.inflight_growth_ticks = state.inflight_growth_ticks + 1 if inflight > state.inflight_last else 0
        state.inflight_last = inflight

        pre_success = pre_succeeded / pre_done if pre_done else None
        stable_success = stable_succeeded / stable_done if stable_done else None
        if pre_success is not None:
            PRERELEASE_SUCCESS_RATIO.set(pre_success)
        if stable_success is not None:
            STABLE_SUCCESS_RATIO.set(stable_success)

        elapsed = now - state.stepStartedAt
        if elapsed < self.settings.min_hold_seconds or pre_done < self.settings.min_samples:
            if elapsed > self.settings.step_timeout_seconds:
                return self._abort(state, "timeout", elapsedSeconds=round(elapsed), preDone=pre_done)
            reason = "min_hold" if elapsed < self.settings.min_hold_seconds else "min_samples"
            return self._hold(state, reason, elapsedSeconds=round(elapsed), preDone=pre_done)

        # (gate, passed, observed, threshold)
        gates = [("success_ratio", pre_success >= self.settings.min_success_ratio,
                  pre_success, self.settings.min_success_ratio)]
        if stable_success is not None:
            minimum = stable_success - self.settings.max_success_diff
            gates.append(("stable_comparison", pre_success >= minimum, pre_success, minimum))
        gates.append(("stuck_jobs", state.inflight_growth_ticks < STUCK_TICKS,
                      state.inflight_growth_ticks, STUCK_TICKS))
        for gate, passed, _, _ in gates:
            ROLLOUT_GATE_PASSED.labels(gate=gate).set(int(passed))

        failed = next((gate for gate in gates if not gate[1]), None)
        if failed is not None:
            gate, _, observed, threshold = failed
            log_stage(logger, "rollout.gate_failed", "Rollout gate failed", level=logging.ERROR,
                      gate=gate, observed=observed, threshold=threshold, **self._fields(state))
            return self._abort(state, f"gate:{gate}")

        if state.stepIndex == len(self.steps) - 1:
            state.status = PROMOTED
            state.percent = 100
            self.store.save(state)
            log_stage(logger, "rollout.promoted", "Rollout promoted", preSuccess=pre_success,
                      **self._fields(state))
            return "promote"

        state.stepIndex += 1
        state.percent = self.steps[state.stepIndex]
        self._begin_step(state, pre, stable, now)
        self.store.save(state)
        log_stage(logger, "rollout.step_advanced", "Rollout advanced to next step", preSuccess=pre_success,
                  stableSuccess=stable_success, **self._fields(state))
        return "advance"

    def _begin_step(self, state: RolloutState, pre: Probe, stable: Probe, now: float) -> None:
        state.stepStartedAt = now
        state.pre_succeeded, state.pre_failed, state.pre_created = pre.succeeded, pre.failed, pre.created
        state.stable_succeeded, state.stable_failed = stable.succeeded, stable.failed
        state.inflight_last = 0
        state.inflight_growth_ticks = 0

    def _hold(self, state: RolloutState, reason: str, save: bool = True, **fields) -> str:
        if save:
            self.store.save(state)
        log_stage(logger, "rollout.hold", "Holding rollout", holdReason=reason, **fields, **self._fields(state))
        return "hold"

    def _abort(self, state: RolloutState, reason: str, **fields) -> str:
        state.status = ABORTED
        state.percent = 0
        state.reason = reason
        self.store.save(state)
        log_stage(logger, "rollout.aborted", "Rollout aborted", level=logging.ERROR, reason=reason,
                  **fields, **self._fields(state))
        return "abort"

    @staticmethod
    def _fields(state: RolloutState) -> dict:
        return {"rolloutVersion": state.version, "status": state.status,
                "stepIndex": state.stepIndex, "percent": state.percent}
