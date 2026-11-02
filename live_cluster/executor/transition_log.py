"""Write-ahead transition log. plan.md §6.3.

APPEND-ONLY means append a TRANSITION RECORD — never rewrite a line. Each state
change is its own line; the in-memory index folds the log into latest-state per
decision_id. Rewriting in place would defeat both the append-only property and
the audit trail, and would throw away the per-stage timing the log gives free.

The fsync BEFORE the side effect is what makes this write-ahead at all. A
buffered write lost in a crash voids the entire guarantee.
"""

import json
import os
import threading
import time

ACCEPTED = "accepted"
APPLYING = "applying"
COMPLETED = "completed"
FAILED = "failed"
REJECTED = "rejected"
RECONCILED = "reconciled"


class TransitionLog:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._latest = {}      # decision_id -> last record
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._fold()

    def _fold(self):
        """Rebuild latest-state from the log. seen_decision_ids derives from
        this, so persistence is not optional."""
        if not os.path.exists(self.path):
            return
        with open(self.path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if rec.get("decision_id"):
                    self._latest[rec["decision_id"]] = rec

    def append(self, decision_id, state, **fields):
        rec = dict(decision_id=decision_id, state=state, ts=time.time(), **fields)
        with self._lock:
            with open(self.path, "a") as fh:
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
                fh.flush()
                os.fsync(fh.fileno())      # write-ahead or it is nothing
            self._latest[decision_id] = rec
        return rec

    def state_of(self, decision_id):
        with self._lock:
            rec = self._latest.get(decision_id)
            return rec["state"] if rec else None

    def seen(self, decision_id):
        return self.state_of(decision_id) is not None

    def orphans(self):
        """decision_ids whose latest state is `applying` — the executor died
        mid-apply and the audit trail is ambiguous until reconciled."""
        with self._lock:
            return [d for d, r in self._latest.items() if r["state"] == APPLYING]

    def applied_for_target(self, target):
        """(decided_at, decision_id) for terminal applications against a
        target. Supersede ordering comes from decided_at (§6.2)."""
        with self._lock:
            out = []
            for d, r in self._latest.items():
                if r.get("target") == target and r["state"] in (COMPLETED, APPLYING):
                    out.append((r.get("decided_at", 0.0), d))
            return out

    def records(self):
        with self._lock:
            return dict(self._latest)
