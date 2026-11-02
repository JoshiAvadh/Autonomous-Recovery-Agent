"""Open-incident suppression and incident identity. plan.md §5.4.

A memory leak satisfies its rule on every window, so re-emitting a diagnosis
every second would flood the policy. One OPEN INCIDENT per (node, type),
opened on first diagnosis and closed when the node is healthy again for ≥3
consecutive samples.

The incident — not the health sample, not the state version — is the unit of
work. Its id anchors every downstream decision (§6.1) and is the key the
evaluator groups on (§8.1a).
"""

import time

# Close an incident after the diagnosis has been ABSENT this many cycles.
#
# MEASURED FIX. This used to close on the node's self-reported `status` being
# healthy for 3 cycles, which was the wrong signal: `status` is derived from
# the health score, and a latency spike never moves it. So every brief dip
# below the 3x threshold closed the incident and the next sample opened a new
# one — one injected spike produced THREE incidents, two of which the
# evaluator then counted as false positives.
#
# Absence of the diagnosis is the only thing that actually means "over".
#
# RAISED 5 -> 12 (2026-10-11). Rule 5 now requires ALL peers elevated (§5.2c),
# which is strict by design — it is what separates the delayed node from a mere
# observer. But strictness means one peer dipping below threshold for a moment
# breaks the condition, and five such cycles closed the incident mid-fault. A
# single 30s latency injection produced TWO incidents 28s apart, the second
# counted as a false positive. The suppression window has to outlast the
# flicker that the attribution rule's strictness introduces.
CLOSE_AFTER_ABSENT = 12


class Incident:
    def __init__(self, target, failure_type, opened_at):
        self.target = target
        self.failure_type = failure_type
        self.opened_at = opened_at
        self.incident_id = "%s:%s:%d" % (target, failure_type, int(opened_at * 1000))
        self.attempt_no = 0
        self.absent_streak = 0
        self.closed_at = None
        self.last_diagnosis = None

    def next_attempt(self):
        """1 = initial decision, 2+ = feedback follow-up (§6.1)."""
        self.attempt_no += 1
        return self.attempt_no

    def to_dict(self):
        return {
            "incident_id": self.incident_id,
            "target": self.target,
            "failure_type": self.failure_type,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "attempts": self.attempt_no,
        }


class IncidentTracker:
    def __init__(self):
        self._open = {}       # (target, failure_type) -> Incident
        self._closed = []

    def observe(self, diagnoses, latest, now=None):
        """Fold this cycle's diagnoses into incidents.

        Returns only NEWLY OPENED incidents — re-diagnosis of something
        already open emits nothing, which is the whole point.
        """
        now = now if now is not None else time.time()
        new = []
        seen = set()

        for d in diagnoses:
            key = (d.target, d.failure_type)
            seen.add(key)
            inc = self._open.get(key)
            if inc is None:
                inc = Incident(d.target, d.failure_type, now)
                self._open[key] = inc
                new.append(inc)
            inc.absent_streak = 0
            inc.last_diagnosis = d

        # Close on the diagnosis being ABSENT, not on the node's self-reported
        # status — see CLOSE_AFTER_ABSENT. `latest` stays in the signature
        # because Phase 6 uses it, and because a caller passing it is a useful
        # reminder that node status is available and deliberately unused here.
        for key, inc in list(self._open.items()):
            if key in seen:
                continue
            inc.absent_streak += 1
            if inc.absent_streak >= CLOSE_AFTER_ABSENT:
                inc.closed_at = now
                self._closed.append(inc)
                del self._open[key]

        return new

    def open_incidents(self):
        return list(self._open.values())

    def get(self, target, failure_type):
        return self._open.get((target, failure_type))

    def summary(self):
        return {
            "open": [i.to_dict() for i in self._open.values()],
            "closed": len(self._closed),
        }
