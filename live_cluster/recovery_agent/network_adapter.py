"""RealNetworkAdapter — makes live telemetry look like the simulator's Network.

plan.md §7 Phase 5. The reused decision code (rule_based.py, llm_agent.py,
scorer.py) is imported UNMODIFIED, so this has to satisfy the exact surface it
reads. Two details that are easy to get wrong and would fail silently:

  - get_stats() must return PERCENT-FORMATTED STRINGS ("92.3%"), because
    scorer.py:102-105 parses them with .replace("%", ""). Returning floats
    raises deep inside scoring, long after the decision was made.

  - .replica_lag must track apply_lag and NEVER match_lag (§4.1b). The reused
    code's dominant use is the stale-read check, which is a read-freshness
    question; match_lag answers a different one.

Phase 5 is read-only, so the mutators raise. They start publishing intents in
Phase 6 — and returning a cheerful no-op here would let a Phase 6 bug hide.
"""

import time


class NodeView:
    """Duck-types the simulator's Node for the fields the policy reads."""

    __slots__ = ("id", "role", "status", "health", "load", "queue_length",
                 "replica_lag", "apply_lag", "boot_id", "started_at",
                 "raft_leader", "has_quorum", "commit_index", "raft_term")

    def __init__(self, msg, stale=False):
        self.id = msg.get("node_id")
        self.role = msg.get("role", "Worker")
        self.status = "failed" if stale else msg.get("status", "healthy")
        self.health = 0.0 if stale else float(msg.get("health", 0.0))
        self.load = float(msg.get("load", 0.0))
        self.queue_length = int(msg.get("queue_length", 0))
        self.apply_lag = int(msg.get("apply_lag", 0))
        self.replica_lag = self.apply_lag        # alias, never match_lag
        self.boot_id = msg.get("boot_id", "")
        self.started_at = float(msg.get("started_at", 0.0))
        self.raft_leader = bool(msg.get("raft_leader"))
        self.has_quorum = bool(msg.get("has_quorum", True))
        self.commit_index = int(msg.get("commit_index", 0))
        self.raft_term = int(msg.get("raft_term", 0))

    def __repr__(self):
        return "NodeView(%s, %s, %s, health=%.0f)" % (
            self.id, self.role, self.status, self.health)


def _pct(numerator, denominator):
    if not denominator:
        return "0%"
    return "%.1f%%" % (100.0 * numerator / denominator)


class RealNetworkAdapter:
    def __init__(self, history, shed_nodes=None):
        self.h = history
        self._shed = set(shed_nodes or ())

    # ------------------------------------------------------------- readers

    @property
    def nodes(self):
        now = time.time()
        stale = set(self.h.stale(now))
        out = {}
        for node_id, msg in self.h.latest_all().items():
            out[node_id] = NodeView(msg, stale=node_id in stale)
        return out

    def get_stats(self):
        """Percent STRINGS — scorer.py parses these with .replace('%','')."""
        latest = self.h.latest_all()
        ok = sum((d.get("req_ok") or 0) for d in latest.values())
        fail = sum((d.get("req_fail") or 0) for d in latest.values())
        timeout = sum((d.get("req_timeout") or 0) for d in latest.values())
        total = ok + fail + timeout

        leader = self.h.leader()
        commit = max([d.get("commit_index", 0) for d in latest.values()] or [0])
        return {
            # Windowed, not cumulative — §8.3 records why the simulator's
            # cumulative success_rate made its delivery_score inert.
            "success_rate": _pct(ok, total),
            "commit_rate": _pct(ok, total),
            "timeout_rate": _pct(timeout, total),
            "total_messages": total,
            "delivered": ok,
            "dropped": fail + timeout,
            "current_primary": leader,
            "commit_index": commit,
            "leader_elections": max(
                [d.get("raft_term", 0) for d in latest.values()] or [0]),
            "split_brain_events": self.split_brain_violations(),
        }

    def consensus_snapshot(self):
        latest = self.h.latest_all()
        leader = self.h.leader()
        return {
            "current_primary": leader,
            "has_quorum": bool((latest.get(leader) or {}).get("has_quorum", False))
            if leader else False,
            "commit_index": max(
                [d.get("commit_index", 0) for d in latest.values()] or [0]),
            "leader_elections": max(
                [d.get("raft_term", 0) for d in latest.values()] or [0]),
            "split_brain_events": self.split_brain_violations(),
        }

    def split_brain_violations(self):
        """§4.7: two DISTINCT nodes claiming leadership in the SAME term.

        Expected 0. Cross-term dual leadership is the benign stale-leader
        window and is deliberately NOT counted here — conflating the two would
        report correct Raft behaviour as a safety violation.
        """
        by_term = {}
        for node_id, d in self.h.latest_all().items():
            if d.get("raft_leader"):
                by_term.setdefault(d.get("raft_term", 0), set()).add(node_id)
        return sum(1 for nodes in by_term.values() if len(nodes) > 1)

    def is_node_shed(self, node_id):
        return node_id in self._shed

    # ------------------------------------------------------------ mutators

    def _readonly(self, name):
        raise NotImplementedError(
            "%s is read-only in Phase 5. Mutators publish intents to "
            "cluster.actions from Phase 6 (plan.md §6)." % name)

    def partition(self, *a, **k):
        self._readonly("partition")

    def heal_partition(self, *a, **k):
        self._readonly("heal_partition")

    def rejoin_node(self, *a, **k):
        self._readonly("rejoin_node")

    def elect_new_primary(self, *a, **k):
        self._readonly("elect_new_primary")

    def set_primary(self, *a, **k):
        self._readonly("set_primary")
