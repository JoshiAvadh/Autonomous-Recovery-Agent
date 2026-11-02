"""Fixture-driven classifier tests. plan.md §9 — Docker-free, high value/line.

Every case here encodes something Phase 4 MEASURED, so a regression shows up
as a failing assertion rather than as a quietly wrong accuracy number three
phases later.

    python3 tests/test_classifier.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recovery_agent.classifier import (  # noqa: E402
    AMBIGUOUS, CPU, CRASH, LATENCY, LEAK, PARTITION, Classifier,
)
from recovery_agent.history import ClusterHistory  # noqa: E402

T0 = 1_000_000.0
passed, failed = [], []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label,
                          (" — " + detail) if detail else ""))


def msg(node, t, **kw):
    d = dict(node_id=node, role=kw.pop("role", "Replica"),
             status=kw.pop("status", "healthy"), health=kw.pop("health", 100.0),
             load=kw.pop("load", 5.0), ts=t, boot_id=kw.pop("boot_id", node + "-boot"),
             started_at=T0, uptime=t - T0, queue_length=0,
             raft_leader=kw.pop("raft_leader", False), raft_term=kw.pop("raft_term", 5),
             has_quorum=kw.pop("has_quorum", True), commit_index=kw.pop("commit_index", 100),
             apply_lag=0, replica_lag=0, match_lag=kw.pop("match_lag", None),
             cpu_pct=kw.pop("cpu_pct", 5.0), rss_mb=kw.pop("rss_mb", 40.0),
             rss_baseline_mb=kw.pop("rss_baseline_mb", 40.0),
             req_ok=kw.pop("req_ok", 20), req_fail=0, req_timeout=0,
             req_p50_ms=kw.pop("req_p50_ms", 1.0), req_p99_ms=kw.pop("req_p99_ms", 2.0),
             peers_reachable=kw.pop("peers_reachable", None),
             peer_rtt_ms=kw.pop("peer_rtt_ms", None),
             peer_rtt_baseline_ms=kw.pop("peer_rtt_baseline_ms", None))
    d.update(kw)
    return d


def fresh(warmup=0.0):
    h = ClusterHistory(warmup_s=warmup)
    return h, Classifier(h)


def steady(h, nodes, start, count, step=1.0, **kw):
    """Feed `count` healthy samples for each node."""
    for i in range(count):
        for n in nodes:
            h.ingest(msg(n, start + i * step, **kw))
    return start + count * step


def diagnose_n(c, h, times, nodes, gen, **kw):
    """Run `times` diagnose cycles, returning the last non-empty result."""
    out = []
    for t in times:
        for n in nodes:
            h.ingest(gen(n, t))
        out = c.diagnose(t)
    return out


# ---------------------------------------------------------------- warm-up

def test_warmup_suppresses():
    h, c = fresh(warmup=60.0)
    t = T0
    for i in range(20):
        h.ingest(msg("node-1", t + i, cpu_pct=99.0, status="degraded", health=40))
    out = c.diagnose(t + 19)
    check("warm-up: nothing emitted in the first 60s", out == [], repr(out))


def test_boot_id_change_rewarms():
    h, c = fresh(warmup=30.0)
    t = steady(h, ["node-1"], T0, 40)
    # New boot -> re-warm. A fresh container's RSS climbs as caches refill;
    # without this, the leak rule fires seconds after EVERY recovery.
    h.ingest(msg("node-1", t, boot_id="node-1-boot2", rss_mb=300.0))
    hist = h.node("node-1")
    check("boot_id change re-enters warm-up", not hist.warm(t),
          "warm_from=%s now=%s" % (hist.warm_from, t))


# ------------------------------------------------------------------ crash

def test_crash_needs_peer_agreement():
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    # node-1 goes silent; peers report it unreachable. Process death closes
    # sockets cleanly, so peers_reachable IS trustworthy here (unlike DROP).
    out = []
    for i in range(4):
        tt = t + 6 + i
        for n in ("node-2", "node-3"):
            h.ingest(msg(n, tt, peers_reachable={"node-1": False}))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("crash: silent + all peers agree unreachable", (CRASH, "node-1") in kinds, str(kinds))


def test_silent_but_reachable_is_ambiguous():
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    out = []
    for i in range(4):
        tt = t + 6 + i
        for n in ("node-2", "node-3"):
            h.ingest(msg(n, tt, peers_reachable={"node-1": True}))
        out = c.diagnose(tt)
    d = [x for x in out if x.target == "node-1"]
    check("silent but peers reach it -> ambiguous, NOT crash",
          bool(d) and d[0].failure_type == AMBIGUOUS and d[0].ambiguous,
          str([(x.failure_type, x.target) for x in out]))


def test_serving_tier_crash_without_corroboration():
    """MEASURED GAP: readonly nodes are anonymous to Raft peers (§4.6a), so no
    voter can name them and `known` is always 0. The first Phase 5 run missed
    a real crash of node-4 for exactly this reason."""
    h, c = fresh()
    t = T0
    for i in range(5):
        for n in ("node-1", "node-2"):
            h.ingest(msg(n, t + i))                      # voters
        h.ingest(msg("node-4", t + i, role="Worker"))    # serving tier
    t += 5
    out = []
    for i in range(4):
        tt = t + 6 + i
        # voters publish, but say nothing about node-4 — they cannot name it
        for n in ("node-1", "node-2"):
            h.ingest(msg(n, tt, peers_reachable={"node-1": True, "node-2": True}))
        out = c.diagnose(tt)
    d = [x for x in out if x.target == "node-4"]
    check("serving-tier crash detected without peer corroboration",
          bool(d) and d[0].failure_type == CRASH and d[0].confidence < 0.9,
          str([(x.failure_type, x.target, x.confidence) for x in out]))


def test_missing_peer_row_is_unknown_not_unreachable():
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    out = []
    for i in range(4):
        tt = t + 6 + i
        for n in ("node-2", "node-3"):
            h.ingest(msg(n, tt, peers_reachable={}))   # no opinion at all
        out = c.diagnose(tt)
    check("missing peer row = unknown, emits no crash",
          not any(d.failure_type == CRASH for d in out),
          str([(d.failure_type, d.target) for d in out]))


# -------------------------------------------------------------- partition

def test_partition_on_term_divergence_and_match_lag():
    """§5.5a: the signals Phase 4 measured (term 24 vs 1, match_lag 0->69)."""
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    out = []
    for i in range(4):
        tt = t + 6 + i
        h.ingest(msg("node-1", tt, raft_term=24, has_quorum=False))
        h.ingest(msg("node-2", tt, raft_term=5, raft_leader=True, role="Primary",
                     match_lag={"node-1": 40 + i * 10, "node-3": 0}))
        h.ingest(msg("node-3", tt, raft_term=5))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("partition: term divergence + match_lag growth",
          (PARTITION, "node-1") in kinds, str(kinds))


def test_no_partition_from_peers_reachable_alone():
    """peers_reachable must NOT be sufficient — §5.5a showed it is conditional
    on connection churn, so a rule keyed on it fires in the obvious case and
    stays silent in the subtle one."""
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    out = []
    for i in range(5):
        tt = t + 6 + i
        h.ingest(msg("node-1", tt, peers_reachable={"node-2": False, "node-3": False}))
        h.ingest(msg("node-2", tt, raft_leader=True, role="Primary",
                     peers_reachable={"node-1": False}))
        h.ingest(msg("node-3", tt, peers_reachable={"node-1": False}))
        out = c.diagnose(tt)
    check("peers_reachable=False alone does NOT declare a partition",
          not any(d.failure_type == PARTITION for d in out),
          str([(d.failure_type, d.target) for d in out]))


def test_absorbed_single_link_partition_is_silent():
    """Raft routes around a single-link cut, so reporting nothing is CORRECT.
    Scoring it as a miss would punish the classifier for being right."""
    h, c = fresh()
    t = steady(h, ["node-1", "node-2", "node-3"], T0, 5)
    out = []
    for i in range(5):
        tt = t + 6 + i
        # post-election steady state: one term bump, everyone agrees, lag 0
        h.ingest(msg("node-1", tt, raft_term=14))
        h.ingest(msg("node-2", tt, raft_term=14))
        h.ingest(msg("node-3", tt, raft_term=14, raft_leader=True, role="Primary",
                     match_lag={"node-1": 0, "node-2": 0}))
        out = c.diagnose(tt)
    check("absorbed single-link partition emits nothing", out == [],
          str([(d.failure_type, d.target) for d in out]))


# ------------------------------------------------------------------- leak

def test_leak_requires_r2_and_samples():
    h, c = fresh()
    t = T0
    out = []
    for i in range(14):
        tt = t + i
        h.ingest(msg("node-4", tt, role="Worker", rss_mb=40.0 + i * 8.0,
                     rss_baseline_mb=40.0))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("leak: monotonic rss climb over baseline", (LEAK, "node-4") in kinds, str(kinds))


def test_noisy_rss_is_not_a_leak():
    h, c = fresh()
    t = T0
    out = []
    for i in range(14):
        tt = t + i
        jitter = [0, 9, -6, 4, -8, 7, -3, 6, -7, 2, -4, 8, -5, 3][i]
        h.ingest(msg("node-4", tt, role="Worker", rss_mb=60.0 + jitter,
                     rss_baseline_mb=40.0))
        out = c.diagnose(tt)
    check("noisy rss (low r2) is NOT a leak",
          not any(d.failure_type == LEAK for d in out),
          str([(d.failure_type, d.target) for d in out]))


# -------------------------------------------------------------------- cpu

def test_cpu_overload():
    h, c = fresh()
    t = T0
    out = []
    for i in range(8):
        tt = t + i
        h.ingest(msg("node-5", tt, role="Gateway", cpu_pct=102.0))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("cpu_overload: sustained above threshold", (CPU, "node-5") in kinds, str(kinds))


# ---------------------------------------------------------------- latency

def test_latency_abstains_without_rtt_baseline():
    """No baseline yet -> ABSTAIN. Never compare against a number that does
    not exist (§5.5)."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-2", tt,
                     peer_rtt_ms={"node-1": 300.0, "node-3": 300.0},
                     peer_rtt_baseline_ms={"node-1": None, "node-3": None}))
        out = c.diagnose(tt)
    check("latency: abstains with no RTT baseline",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def test_latency_fires_on_peer_rtt():
    """§5.2b: the signal req_p99_ms could not carry. 150ms on a ~1ms baseline
    is a 150x signal, vs the 2.27x the write path produced."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-2", tt,
                     peer_rtt_ms={"node-1": 151.0, "node-3": 149.0},
                     peer_rtt_baseline_ms={"node-1": 1.0, "node-3": 1.1}))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("latency: fires on peer RTT 150ms vs 1ms baseline",
          (LATENCY, "node-2") in kinds, str(kinds))


def test_cpu_starvation_is_not_latency():
    """MEASURED FP. The probe runs in-process, so a CPU burn starves it and
    inflates this node's own outbound RTT to EVERY peer. node-5 reported
    "rtt to 4/4 peers elevated" 3s before cpu_overload confirmed. In-process
    probing cannot tell "the path is slow" from "I am too busy to measure it",
    so a saturated node must defer to cpu_overload."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-5", tt, role="Gateway", cpu_pct=101.0,
                     peer_rtt_ms={"node-1": 116.0, "node-2": 120.0, "node-3": 118.0},
                     peer_rtt_baseline_ms={"node-1": 1.0, "node-2": 1.0, "node-3": 1.0}))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("CPU starvation is cpu_overload, NOT latency_spike",
          (CPU, "node-5") in kinds and (LATENCY, "node-5") not in kinds, str(kinds))


def test_exactly_half_elevated_does_not_fire():
    """MEASURED FP. 2-of-4 and 2-of-3 both leaked through a `< measured` test.
    Half is not most; a strict majority is required."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-1", tt,
                     peer_rtt_ms={"node-2": 154.0, "node-5": 264.0,
                                  "node-3": 1.0, "node-4": 1.1},
                     peer_rtt_baseline_ms={"node-2": 1.0, "node-5": 1.0,
                                           "node-3": 1.0, "node-4": 1.0}))
        out = c.diagnose(tt)
    check("exactly half the peers elevated does NOT fire",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def test_observer_of_two_slow_peers_is_not_blamed():
    """MEASURED FP. Latency on node-3 plus a CPU-starved node-5 made node-2 see
    2-of-3 peers slow — a strict majority — and it was wrongly diagnosed. It
    was observing two independently slow peers. One still-fast peer proves the
    local path is fine, so ALL measured peers must be elevated."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-2", tt,
                     peer_rtt_ms={"node-3": 153.0, "node-5": 193.0, "node-1": 1.0},
                     peer_rtt_baseline_ms={"node-3": 1.0, "node-5": 1.0, "node-1": 1.0}))
        out = c.diagnose(tt)
    check("observer of two slow peers is NOT the subject",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def test_all_peers_elevated_is_the_subject():
    """The real target sees EVERY peer slow, because its own path is delayed."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-3", tt,
                     peer_rtt_ms={"node-1": 151.0, "node-2": 158.0, "node-5": 193.0},
                     peer_rtt_baseline_ms={"node-1": 1.0, "node-2": 1.0, "node-5": 1.2}))
        out = c.diagnose(tt)
    kinds = [(d.failure_type, d.target) for d in out]
    check("all peers elevated -> this node IS the subject",
          (LATENCY, "node-3") in kinds, str(kinds))


def test_single_slow_link_is_not_blamed_on_the_node():
    """Elevated to ONE peer is a link problem, not this node's path. Blaming
    the node would produce a wrong_target the evaluator counts separately."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-2", tt,
                     peer_rtt_ms={"node-1": 400.0, "node-3": 1.0, "node-4": 1.1},
                     peer_rtt_baseline_ms={"node-1": 1.0, "node-3": 1.0, "node-4": 1.0}))
        out = c.diagnose(tt)
    check("latency: one slow link does NOT blame the node",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def test_rtt_timeout_is_not_a_latency_sample():
    """An unreachable peer yields no sample at all, so a crash cannot read as
    a latency spike."""
    h, c = fresh()
    out = []
    for i in range(6):
        tt = T0 + i
        h.ingest(msg("node-2", tt,
                     peer_rtt_ms={"node-1": None, "node-3": 1.0},
                     peer_rtt_baseline_ms={"node-1": 1.0, "node-3": 1.0}))
        out = c.diagnose(tt)
    check("latency: an unreachable peer is not a latency sample",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def _superseded_test_promotion_is_not_a_latency_spike():
    """MEASURED FIX. Phase 5 gate run 2: latency injected on the leader cost it
    leadership, node-2 was promoted, and its p99 went 0.03ms -> 198ms (the
    quorum round-trip). Against its own FOLLOWER history that read as a 6585x
    spike — a false positive on the wrong node from a regime change, not a
    fault. Leadership determines the latency regime, so baseline samples must
    come from the same regime."""
    h, c = fresh()
    # long history as a FOLLOWER: fast local reads
    for i in range(80):
        h.ingest(msg("node-2", T0 + i, raft_leader=False, req_p99_ms=0.03, req_ok=25))
    out = []
    for i in range(6):
        tt = T0 + 80 + i
        # promoted: now serving writes at real quorum latency
        h.ingest(msg("node-2", tt, raft_leader=True, role="Primary",
                     req_p99_ms=198.0, req_ok=25))
        out = c.diagnose(tt)
    check("promotion to leader is NOT a latency spike",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


def _superseded_test_tiny_absolute_jump_is_not_a_spike():
    h, c = fresh()
    for i in range(80):
        h.ingest(msg("node-2", T0 + i, req_p99_ms=0.03, req_ok=25))
    out = []
    for i in range(6):
        tt = T0 + 80 + i
        h.ingest(msg("node-2", tt, req_p99_ms=0.30, req_ok=25))   # 10x, but 0.3ms
        out = c.diagnose(tt)
    check("10x of a sub-ms baseline is not a spike (absolute floor)",
          not any(d.failure_type == LATENCY for d in out),
          str([(d.failure_type, d.target) for d in out]))


# ------------------------------------------------------------- incidents

def test_flickering_diagnosis_stays_one_incident():
    """MEASURED FIX. A latency spike never moves the node's `status` field, so
    closing on 'node healthy for 3 cycles' closed the incident on every brief
    dip below the threshold. One injected spike produced THREE incidents, two
    of which the evaluator counted as false positives."""
    from recovery_agent.incidents import IncidentTracker
    from recovery_agent.classifier import Diagnosis

    tr = IncidentTracker()
    latest = {"node-1": {"node_id": "node-1", "status": "healthy"}}
    d = Diagnosis("node-1", LATENCY, 0.75, ["spike"])

    opened = tr.observe([d], latest, T0)
    # flicker: present, absent, present, absent, present
    for i, present in enumerate([False, True, False, True], start=1):
        opened += tr.observe([d] if present else [], latest, T0 + i)
    check("flickering diagnosis stays ONE incident", len(opened) == 1,
          "%d incidents opened" % len(opened))


def test_incident_closes_when_really_over():
    from recovery_agent.incidents import IncidentTracker
    from recovery_agent.classifier import Diagnosis

    tr = IncidentTracker()
    latest = {"node-1": {"node_id": "node-1", "status": "healthy"}}
    d = Diagnosis("node-1", LATENCY, 0.75, ["spike"])
    tr.observe([d], latest, T0)
    for i in range(1, 16):          # CLOSE_AFTER_ABSENT is 12
        tr.observe([], latest, T0 + i)
    check("incident closes once the diagnosis is really gone",
          len(tr.open_incidents()) == 0 and tr.summary()["closed"] == 1,
          str(tr.summary()))


def main():
    print("=== classifier fixture tests ===\n")
    for fn in sorted(
        (v for k, v in globals().items() if k.startswith("test_")),
        key=lambda f: f.__code__.co_firstlineno,
    ):
        try:
            fn()
        except Exception as exc:
            check(fn.__name__, False, "raised %s: %s" % (type(exc).__name__, exc))
    print("\n  %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
