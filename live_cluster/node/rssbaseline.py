"""RSS baseline for the memory-leak signal. plan.md §5.5.

The baseline is the MEDIAN RSS over the 30-60s window after process start —
never the first reading, which is taken before caches, Kafka buffers and gRPC
pools have filled and would therefore sit far below true steady state. A
too-low baseline makes ordinary warm-up look like a leak.

Until the window closes the baseline is None, and the classifier's leak rule
abstains rather than comparing against a number that does not exist yet.

The node owns this rather than the agent because only the node knows its own
process start. The agent re-warms per §5.5 on a boot_id change, which it can
see; the node simply restarts and begins a fresh window.
"""

import statistics
import threading
import time

WINDOW_START_S = 30.0
WINDOW_END_S = 60.0


class RssBaseline:
    def __init__(self, started_at, window=(WINDOW_START_S, WINDOW_END_S)):
        self._started_at = started_at
        self._lo, self._hi = window
        self._samples = []
        self._value = None
        self._lock = threading.Lock()

    def observe(self, rss_mb, now=None):
        """Feed one sample. Cheap enough to call every publish cycle."""
        now = now if now is not None else time.time()
        age = now - self._started_at
        with self._lock:
            if self._value is not None:
                return
            if self._lo <= age <= self._hi:
                self._samples.append(rss_mb)
            elif age > self._hi and self._samples:
                self._value = round(statistics.median(self._samples), 2)
                self._samples = []

    @property
    def value(self):
        """Median RSS over the warm-up window, or None while still warming.

        None is load-bearing: it is how the leak rule knows to abstain. Never
        substitute the current RSS as a placeholder — that would make the
        delta zero and silence the signal permanently.
        """
        with self._lock:
            return self._value

    def ready(self):
        return self.value is not None
