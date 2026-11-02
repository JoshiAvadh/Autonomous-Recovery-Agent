"""Self-inflicted faults: memory leak and CPU burn. plan.md Phase 4.

Crash, partition and latency have external levers (docker kill, iptables, tc).
Memory leak and CPU overload do not — you cannot make an otherwise-healthy
container leak from outside it. So the injector tells the node to genuinely
misbehave, and it does: real allocation, real cycles burned.

plan.md is emphatic on this point: "real resource consumption, not a flag
flip". A boolean would make rss_mb and cpu_pct lie, and those two fields are
exactly what the Phase 5 classifier reads to tell a leak from an overload.
"""

import logging
import threading
import time

log = logging.getLogger("behaviors")

# Touched after allocation so the pages are actually resident: an untouched
# allocation may never show up in RSS, which would make the leak invisible to
# the very telemetry it exists to move.
_CHUNK_BYTES = 1024 * 1024


class MemoryLeak:
    def __init__(self):
        self._blocks = []
        self._thread = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.rate_mb_s = 0.0

    @property
    def active(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self, rate_mb_s=5.0):
        with self._lock:
            if self.active:
                return False, "leak already running at %.1f MB/s" % self.rate_mb_s
            self.rate_mb_s = max(0.1, float(rate_mb_s))
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="leak", daemon=True)
            self._thread.start()
        log.info("leak started: %.1f MB/s", self.rate_mb_s)
        return True, "leaking %.1f MB/s" % self.rate_mb_s

    def _run(self):
        while not self._stop.is_set():
            chunks = max(1, int(self.rate_mb_s))
            for _ in range(chunks):
                block = bytearray(_CHUNK_BYTES)
                # Touch every page so the allocation is resident, not virtual.
                block[::4096] = b"\xa5" * len(block[::4096])
                self._blocks.append(block)
            self._stop.wait(1.0)

    def stop(self):
        with self._lock:
            if not self.active:
                return False, "no leak running"
            self._stop.set()
        self._thread.join(timeout=3.0)
        freed = len(self._blocks)
        self._blocks = []
        self._thread = None
        log.info("leak stopped, released %d MB", freed)
        return True, "released %d MB" % freed


class CpuBurn:
    def __init__(self):
        self._threads = []
        self._stop = threading.Event()
        self._lock = threading.Lock()

    @property
    def active(self):
        return any(t.is_alive() for t in self._threads)

    def start(self, workers=2):
        with self._lock:
            if self.active:
                return False, "burn already running"
            self._stop.clear()
            self._threads = []
            for i in range(max(1, int(workers))):
                t = threading.Thread(target=self._spin, name="burn%d" % i, daemon=True)
                t.start()
                self._threads.append(t)
        log.info("cpu burn started: %d workers", len(self._threads))
        return True, "burning on %d workers" % len(self._threads)

    def _spin(self):
        # A pure-Python busy loop is GIL-bound, which is the point: it starves
        # the request handlers on this node, so the overload shows up in
        # queue_length and request latency rather than only in cpu_pct.
        x = 0.0
        while not self._stop.is_set():
            for _ in range(200000):
                x = x * 1.000001 + 1.0
            if x > 1e300:
                x = 0.0

    def stop(self):
        with self._lock:
            if not self.active:
                return False, "no burn running"
            self._stop.set()
        for t in self._threads:
            t.join(timeout=3.0)
        self._threads = []
        log.info("cpu burn stopped")
        return True, "burn stopped"


LEAK = MemoryLeak()
BURN = CpuBurn()


def status():
    return {"leaking": LEAK.active, "burning": BURN.active}
