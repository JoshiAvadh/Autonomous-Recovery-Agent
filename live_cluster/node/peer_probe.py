"""Peer round-trip probe. plan.md §5.2b.

Why this exists: `req_p99_ms` CANNOT detect injected network latency, and
Phase 5 measured exactly why. Write latency is an end-to-end number dominated
by a ~154 ms quorum baseline, so a 150 ms path delay only moves it to 349 ms —
2.27x against a 3.0x threshold. Push the delay high enough to clear 3x and it
destabilises leadership instead, which invalidates the baseline. The usable
band is effectively empty.

This measures the path DIRECTLY: each node times its own round trip to each
peer. A 150 ms delay shows up as ~150 ms on a ~1 ms baseline — a 150x signal
instead of 2.27x.

Three properties the write-path measurement could never have:

  1. INDEPENDENT OF LEADERSHIP. RTT does not care who serves writes, so no
     regime filter is needed and a promotion cannot masquerade as a spike.
  2. INDEPENDENT OF THE READ/WRITE MIX. Reads are local and carry no path
     delay (Q1); this probe always crosses the wire.
  3. ATTRIBUTABLE. It yields a matrix, not a scalar. A node whose RTT is
     elevated to MOST of its peers is itself delayed; one peer being slow from
     a single vantage point is a link problem. The write path could only ever
     implicate the leader.
"""

import logging
import threading
import time
from collections import deque

import grpc

log = logging.getLogger("peer_probe")

WINDOW_S = 90.0
PROBE_INTERVAL_S = 1.0
PROBE_TIMEOUT_S = 2.0
# A baseline built inside a fault window is poisoned; require real history.
MIN_BASELINE_SPAN_S = 60.0


class PeerProbe:
    def __init__(self, self_id, peers, port, stub_factory):
        self.self_id = self_id
        self.peers = [p for p in peers if p != self_id]
        self.port = port
        self._stub_factory = stub_factory
        self._samples = {p: deque() for p in self.peers}   # (ts, rtt_ms)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._channels = {}

    def start(self):
        self._thread = threading.Thread(target=self._run, name="peerprobe",
                                        daemon=True)
        self._thread.start()
        log.info("peer probe started: %s", self.peers)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.0)

    def _stub(self, peer):
        ch = self._channels.get(peer)
        if ch is None:
            ch = grpc.insecure_channel("%s:%d" % (peer, self.port))
            self._channels[peer] = ch
        return self._stub_factory(ch)

    def _run(self):
        while not self._stop.is_set():
            for peer in self.peers:
                if self._stop.is_set():
                    break
                t0 = time.time()
                try:
                    self._stub(peer).GetStatus(self._empty(), timeout=PROBE_TIMEOUT_S)
                    rtt = (time.time() - t0) * 1000.0
                except grpc.RpcError:
                    # Unreachable is NOT a latency sample. Recording a timeout
                    # as a huge RTT would make every crash look like a latency
                    # spike — the classifier reads absence of telemetry for
                    # that, not an inflated number.
                    continue
                with self._lock:
                    dq = self._samples[peer]
                    dq.append((t0, rtt))
                    cutoff = t0 - WINDOW_S
                    while dq and dq[0][0] < cutoff:
                        dq.popleft()
            self._stop.wait(PROBE_INTERVAL_S)

    def _empty(self):
        # Set by node_service so this module needs no proto import of its own.
        return self._request

    def bind_request(self, request):
        self._request = request

    def current(self):
        """peer -> most recent RTT in ms (last 5s only, else None)."""
        now = time.time()
        out = {}
        with self._lock:
            for peer, dq in self._samples.items():
                recent = [r for ts, r in dq if ts >= now - 5.0]
                out[peer] = round(max(recent), 2) if recent else None
        return out

    def baseline(self):
        """peer -> median RTT over the window, excluding the last 10s.

        Excluding the recent tail keeps a spike in progress out of its own
        baseline. None until there is enough history to be meaningful.
        """
        now = time.time()
        out = {}
        with self._lock:
            for peer, dq in self._samples.items():
                usable = [(ts, r) for ts, r in dq if ts < now - 10.0]
                old = sorted(r for _, r in usable)
                span = (usable[-1][0] - usable[0][0]) if len(usable) > 1 else 0.0
                # MIN_SPAN as well as a sample count. A peer that rejoined
                # mid-fault accumulates 20 samples quickly, but every one of
                # them is taken DURING the fault — so its "baseline" is the
                # degraded value and it never reads as elevated. Measured: a
                # crashed node rejoining mid-latency-injection grew the peer
                # set 3 -> 4, broke the all-peers-elevated rule, and split one
                # incident into two (the second scored a false positive).
                if len(old) < 20 or span < MIN_BASELINE_SPAN_S:
                    out[peer] = None
                else:
                    mid = len(old) // 2
                    out[peer] = round(
                        old[mid] if len(old) % 2 else (old[mid - 1] + old[mid]) / 2.0,
                        2)
        return out
