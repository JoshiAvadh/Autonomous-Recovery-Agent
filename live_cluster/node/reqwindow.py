"""Windowed request counters and latency percentiles. plan.md §4.1.

WINDOWED, never cumulative. §8.3 records why: the simulator's cumulative
success_rate barely moves over a 5-tick delta, so its delivery_score converges
to a neutral 70 regardless of what action was taken. Live telemetry must not
inherit that defect, so every counter here covers only the last `window`
seconds and older samples are dropped.
"""

import threading
import time
from collections import deque

OK = "ok"
FAIL = "fail"
TIMEOUT = "timeout"


class RequestWindow:
    def __init__(self, window=1.0):
        self._window = window
        self._lock = threading.Lock()
        self._samples = deque()   # (ts, latency_ms, outcome)

    def record(self, latency_ms, outcome):
        now = time.time()
        with self._lock:
            self._samples.append((now, latency_ms, outcome))
            self._prune(now)

    def _prune(self, now):
        cutoff = now - self._window
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def snapshot(self):
        now = time.time()
        with self._lock:
            self._prune(now)
            samples = list(self._samples)

        counts = {OK: 0, FAIL: 0, TIMEOUT: 0}
        latencies = []
        for _, lat, outcome in samples:
            counts[outcome] = counts.get(outcome, 0) + 1
            if outcome == OK:
                latencies.append(lat)

        return {
            "req_ok": counts[OK],
            "req_fail": counts[FAIL],
            "req_timeout": counts[TIMEOUT],
            "req_p50_ms": _pct(latencies, 50),
            "req_p99_ms": _pct(latencies, 99),
            "req_count": len(latencies),
        }


def _pct(values, p):
    """Percentile, or None when there is not enough data.

    None is deliberate and load-bearing: plan.md §5.5 requires the latency rule
    to ABSTAIN below 20 requests rather than fire on a p99 computed from three
    samples. Returning 0.0 here would look like "fast" and silently defeat that.
    """
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 2)
    k = (len(ordered) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    frac = k - lo
    return round(ordered[lo] * (1 - frac) + ordered[hi] * frac, 2)
