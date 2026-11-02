"""Rolling per-node telemetry cache. The agent's entire view of the world.

Fed ONLY by cluster.health (plan.md §1). Ground truth never reaches this
class, or anything downstream of it.

Also owns two pieces of state the classifier and the adapter both need:
  - warm-up (§5.5), keyed on boot_id so a restart re-warms
  - target_generation (§6.2), incremented only on a RELEVANT change
"""

import threading
import time
from collections import deque

# Retain enough for the latency baseline (60s) plus slack for the leak fit.
RETAIN_S = 120.0

# A node that has not published for this long is treated as failed. This is
# how `docker kill` is detected — a dead process publishes nothing (§4.5).
STALE_AFTER_S = 5.0

WARMUP_S = 60.0


class NodeHistory:
    def __init__(self, node_id):
        self.node_id = node_id
        self.samples = deque()          # (ts, dict)
        self.first_seen = None
        self.boot_id = None
        self.warm_from = None           # wall-clock when warm-up ends
        self.generation = 0
        self._last_relevant = None

    def warm(self, now=None):
        now = now if now is not None else time.time()
        return self.warm_from is not None and now >= self.warm_from

    def latest(self):
        return self.samples[-1][1] if self.samples else None

    def window(self, seconds, now=None):
        now = now if now is not None else time.time()
        cutoff = now - seconds
        return [d for ts, d in self.samples if ts >= cutoff]

    def age(self, now=None):
        if not self.samples:
            return None
        now = now if now is not None else time.time()
        return now - self.samples[-1][0]


def _relevant_state(msg):
    """The tuple whose change bumps target_generation (§6.2).

    Deliberately excludes health drift, load jitter and commit_index — those
    move every tick and would churn the generation into meaninglessness.
    """
    return (
        msg.get("status"),
        msg.get("role"),
        msg.get("boot_id"),
        bool(msg.get("shedding")),
        bool(msg.get("isolated")),
    )


class ClusterHistory:
    def __init__(self, retain_s=RETAIN_S, warmup_s=WARMUP_S):
        self._retain = retain_s
        self._warmup = warmup_s
        self._nodes = {}
        self._lock = threading.Lock()

    def ingest(self, msg):
        """Absorb one cluster.health message."""
        node_id = msg.get("node_id")
        if not node_id:
            return
        ts = float(msg.get("ts") or time.time())

        with self._lock:
            h = self._nodes.get(node_id)
            if h is None:
                h = self._nodes[node_id] = NodeHistory(node_id)
                h.first_seen = ts
                h.boot_id = msg.get("boot_id")
                h.warm_from = ts + self._warmup

            # §5.5 post-restart re-warm, keyed on IDENTITY not on uptime
            # decreasing: a node that restarts twice between samples still
            # shows exactly one boot_id change. A fresh container's RSS climbs
            # steeply as caches refill, and without re-warming that satisfies
            # the leak rule within seconds of EVERY recovery — restart ->
            # leak-diagnosis -> restart.
            boot = msg.get("boot_id")
            if boot and boot != h.boot_id:
                h.boot_id = boot
                h.warm_from = ts + self._warmup
                h.samples.clear()

            rel = _relevant_state(msg)
            if h._last_relevant is not None and rel != h._last_relevant:
                h.generation += 1
            h._last_relevant = rel

            h.samples.append((ts, msg))
            cutoff = ts - self._retain
            while h.samples and h.samples[0][0] < cutoff:
                h.samples.popleft()

    # ------------------------------------------------------------- readers

    def nodes(self):
        with self._lock:
            return dict(self._nodes)

    def node(self, node_id):
        with self._lock:
            return self._nodes.get(node_id)

    def latest_all(self):
        with self._lock:
            return {n: h.latest() for n, h in self._nodes.items() if h.latest()}

    def stale(self, now=None, threshold=STALE_AFTER_S):
        """Nodes that have gone quiet — the crash signal."""
        now = now if now is not None else time.time()
        out = []
        with self._lock:
            for n, h in self._nodes.items():
                age = h.age(now)
                if age is not None and age > threshold:
                    out.append(n)
        return sorted(out)

    def live(self, now=None, threshold=STALE_AFTER_S):
        now = now if now is not None else time.time()
        with self._lock:
            return sorted(
                n for n, h in self._nodes.items()
                if h.age(now) is not None and h.age(now) <= threshold
            )

    def leader(self):
        latest = self.latest_all()
        for n, d in latest.items():
            if d.get("raft_leader"):
                return n
        return None

    def warm_report(self, now=None):
        """(node_id -> seconds remaining) for wait_for_warmup.sh."""
        now = now if now is not None else time.time()
        out = {}
        with self._lock:
            for n, h in self._nodes.items():
                if h.warm_from is None:
                    out[n] = self._warmup
                else:
                    out[n] = max(0.0, h.warm_from - now)
        return out

    def all_warm(self, now=None):
        rep = self.warm_report(now)
        return bool(rep) and all(v <= 0.0 for v in rep.values())

    def generation(self, node_id):
        h = self.node(node_id)
        return h.generation if h else 0
