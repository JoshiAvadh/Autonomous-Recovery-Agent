"""Message schemas for every Kafka topic. See plan.md §4.

Fields arrive in stages (plan.md §4.1): base + identity in Phase 1-2, req_* in
Phase 3, rss_* and peers_reachable in Phase 4. Everything not yet produced is
Optional and defaults to None so a Phase-1 message validates against the same
dataclass a Phase-5 consumer reads.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

# ---------------------------------------------------------------- topic names

TOPIC_HEALTH = "cluster.health"
TOPIC_ACTIONS = "cluster.actions"
TOPIC_ACTION_RESULTS = "cluster.action_results"
TOPIC_SCORES = "cluster.scores"
TOPIC_FAILURES = "cluster.failures"
# Diagnoses were only ever LOGGED. Publishing them gives the dashboard the
# full causal chain and frees the evaluator from parsing `docker compose
# logs`, which broke on any format change.
TOPIC_DIAGNOSES = "cluster.diagnoses"

ALL_TOPICS = [
    TOPIC_HEALTH,
    TOPIC_ACTIONS,
    TOPIC_ACTION_RESULTS,
    TOPIC_SCORES,
    TOPIC_FAILURES,
    TOPIC_DIAGNOSES,
]

# The agent MUST NOT subscribe to TOPIC_FAILURES. It is ground truth, consumed
# only by the offline evaluator (plan.md §1). Feeding it to the agent collapses
# the thesis to "given a labeled failure, pick an action".
AGENT_INPUT_TOPICS = [TOPIC_HEALTH]

STATUS_HEALTHY = "healthy"
STATUS_DEGRADED = "degraded"
STATUS_FAILED = "failed"


# --------------------------------------------------------------- cluster.health


@dataclass
class HealthMessage:
    """One node's self-report, published at 1 Hz. plan.md §4.1."""

    node_id: str
    role: str
    status: str
    health: float
    load: float
    ts: float

    # process identity — plan.md §4.1a.
    # boot_id is the ONLY valid restart proof. uptime is a duration and can
    # never answer "is this the same process I decided against".
    boot_id: str = ""
    started_at: float = 0.0
    uptime: float = 0.0

    queue_length: int = 0
    raft_leader: bool = False
    raft_term: int = 0
    # False on the minority side of a majority cut — a primary partition signal
    # once §5.5a showed peers_reachable cannot carry one.
    has_quorum: bool = True
    commit_index: int = 0

    # plan.md §4.1b — two lags, two questions.
    # apply_lag  = leader_commit_idx - last_applied   (self-observed, read freshness)
    # match_lag  = leader_commit_idx - match_idx[n]   (leader-observed, replication)
    # replica_lag is an ALIAS of apply_lag: it is the name the reused simulator
    # code reads (network.py stale-read check). Never alias it to match_lag.
    apply_lag: int = 0
    replica_lag: int = 0
    match_lag: Optional[Dict[str, int]] = None       # leader only
    leader_commit_idx: Optional[int] = None          # leader only

    # Phase 4 — memory_leak / cpu_overload signals
    rss_mb: Optional[float] = None
    rss_baseline_mb: Optional[float] = None
    cpu_pct: Optional[float] = None

    # Phase 3 — windowed over 1s, NEVER cumulative (plan.md §8.3 explains why
    # the simulator's cumulative success_rate makes its delivery_score dead)
    req_p50_ms: Optional[float] = None
    req_p99_ms: Optional[float] = None
    req_ok: Optional[int] = None
    req_fail: Optional[int] = None
    req_timeout: Optional[int] = None

    # §5.2b — DIRECT path measurement, because req_p99_ms provably cannot
    # detect injected latency (write p99 is diluted by the quorum baseline and
    # entangled with leadership). Address-keyed, so unlike the leader's view of
    # the serving tier this names every peer unambiguously.
    peer_rtt_ms: Optional[Dict[str, float]] = None
    peer_rtt_baseline_ms: Optional[Dict[str, float]] = None

    # Phase 2 — projected from SyncObj.getStatus()'s partner/readonly node
    # status keys. A MISSING row means unknown, never "unreachable" — only
    # nodes alive enough to publish assert anything (plan.md §4.1).
    peers_reachable: Optional[Dict[str, bool]] = None

    shedding: bool = False
    isolated: bool = False

    def __post_init__(self):
        # replica_lag is a mirror of apply_lag, not an independent field.
        self.replica_lag = self.apply_lag

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})


# ------------------------------------------------------------- cluster.failures


@dataclass
class FailureInjected:
    """Ground truth, evaluator-only. plan.md §4.2."""

    gt_id: str
    failure_type: str
    target: str
    injected_at: float
    # What the injector INTENDED. Descriptive only — never a time boundary.
    # For crash/leak/cpu it is not even knowable at injection time, because the
    # recovery system under test is what undoes them.
    duration_hint: Optional[float] = None
    event: str = "injected"

    def to_dict(self):
        return asdict(self)


@dataclass
class FailureResolved:
    gt_id: str
    resolved_at: float
    # injector  = authoritative, published when the injector removes the fault
    #             (partition heal, tc qdisc drop)
    # recovery  = DERIVED by the evaluator from cluster.health, because the
    #             recovery system undid it (crash/leak/cpu). Mark as derived.
    # unresolved= run ended with the fault still active
    resolved_by: str = "injector"
    event: str = "resolved"

    def to_dict(self):
        return asdict(self)


# -------------------------------------------------- cluster.actions / results


@dataclass
class ActionIntent:
    """A DECISION EMITTED — never "the action happened". plan.md §6."""

    decision_id: str
    incident_id: str
    attempt_no: int
    action: str
    target: str
    source: str
    confidence: int
    reason: str
    decided_at: float

    expected_target_status: str
    expected_boot_id: str
    expected_started_at: float

    # Agent/evaluator metadata ONLY. The executor holds no generation history
    # and must ignore this — the fresh GetStatus() checks are the actual safety
    # boundary (plan.md §6.2). Supersede ordering comes from decided_at.
    target_generation: int = 0

    diagnosis: Optional[dict] = None

    def to_dict(self):
        return asdict(self)


# accepted -> applying -> completed | failed, plus rejected / reconciled
STATE_ACCEPTED = "accepted"
STATE_APPLYING = "applying"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_REJECTED = "rejected"
STATE_RECONCILED = "reconciled"

# plan.md §6.2a. already_satisfied and stale mean OPPOSITE things downstream:
# already_satisfied says the goal is met, stale says re-decide.
REJECT_DUPLICATE = "duplicate"
REJECT_STALE = "stale"
REJECT_ALREADY_SATISFIED = "already_satisfied"
REJECT_SUPERSEDED = "superseded"
REJECT_UNREACHABLE = "unreachable"


@dataclass
class ActionResult:
    decision_id: str
    incident_id: str
    state: str
    ts: float
    detail: str = ""
    rejected_reason: Optional[str] = None

    def to_dict(self):
        return asdict(self)


# ------------------------------------------------------------------ diagnosis


@dataclass
class FailureDiagnosis:
    """plan.md §5.1. `ambiguous` is a first-class outcome, not a failure —
    forcing a precise type on thin evidence makes the evaluation misleading."""

    target: str
    failure_type: str        # crash | network_partition | latency_spike
                             # | memory_leak | cpu_overload | ambiguous
    confidence: float        # 0.0 - 1.0
    ambiguous: bool
    observed_at: float
    evidence: List[str] = field(default_factory=list)
    target_generation: int = 0   # metadata only — see §6.2

    def to_dict(self):
        return asdict(self)
