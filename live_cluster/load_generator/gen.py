"""Weighted traffic generator. plan.md Phase 3.

This exists because `load` is not a number a node can invent — it is an
emergent consequence of where requests go. Without a real traffic source,
`reroute_traffic` and `rebalance_load` would have nothing to act on and would
have to be logged as no-ops (plan.md §10). The original plan omitted this
service entirely; Phase 3's gate is deliberately blunt about it: change the
weights, watch real load move.

Routing:
  writes -> ALWAYS the current Raft leader (only the leader can commit)
  reads  -> sampled from the weight table

The weight table is the state that `reroute_traffic` and `rebalance_load`
mutate, which is why §6.2a validates those actions against this service rather
than gating them on node reachability.
"""

import logging
import os
import random
import signal
import sys
import threading
import time
from concurrent import futures

import grpc

sys.path.insert(0, "/app")

import loadgen_pb2  # noqa: E402
import loadgen_pb2_grpc  # noqa: E402
import node_pb2  # noqa: E402
import node_pb2_grpc  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("loadgen")

NODES = [n for n in os.getenv("LOADGEN_NODES", "").split(",") if n]
NODE_PORT = int(os.getenv("NODE_GRPC_PORT", "8000"))
LISTEN_PORT = int(os.getenv("LOADGEN_PORT", "9500"))
TARGET_RPS = float(os.getenv("TARGET_RPS", "20"))
WRITE_PCT = float(os.getenv("WRITE_PCT", "0.2"))
WORKERS = int(os.getenv("LOADGEN_WORKERS", "8"))
REQUEST_TIMEOUT_S = float(os.getenv("REQUEST_TIMEOUT_S", "3.0"))
LEADER_REFRESH_S = float(os.getenv("LEADER_REFRESH_S", "2.0"))

_shutdown = threading.Event()
_lock = threading.Lock()

# node_id -> share of READ traffic. Uniform at start (plan.md Phase 3).
_weights = {n: 1.0 / len(NODES) for n in NODES} if NODES else {}
_target_rps = TARGET_RPS
_leader = None

_stats = {"sent": 0, "ok": 0, "failed": 0, "timed_out": 0}
_channels = {}


def channel(node_id):
    ch = _channels.get(node_id)
    if ch is None:
        ch = grpc.insecure_channel("%s:%d" % (node_id, NODE_PORT))
        _channels[node_id] = ch
    return ch


def stub(node_id):
    return node_pb2_grpc.NodeServiceStub(channel(node_id))


def normalised_weights():
    with _lock:
        w = dict(_weights)
    total = sum(v for v in w.values() if v > 0)
    if total <= 0:
        # Every node zeroed. Sending nothing is the honest response — inventing
        # uniform traffic here would mask a shed-everything bug.
        return {}
    return {k: v / total for k, v in w.items() if v > 0}


def pick_read_target():
    w = normalised_weights()
    if not w:
        return None
    r = random.random()
    cum = 0.0
    for node_id, share in w.items():
        cum += share
        if r <= cum:
            return node_id
    return next(iter(w))


def refresh_leader():
    """Poll every node for raft_leader. StatusReply carries the flag for the
    node itself, so the leader is found by asking around rather than trusting
    any single node's opinion of who leads.

    COMPLAINS LOUDLY when no leader is found. A silent failure here is
    genuinely dangerous: with _leader unset, every write is skipped while reads
    keep flowing, so the generator looks perfectly healthy from outside — sent
    and ok both climb — while the write path is entirely dead. That exact
    failure invalidated a whole Phase 4 gate run before anyone noticed, and the
    only visible symptom was the leader's p99 quietly collapsing to read
    latency.
    """
    global _leader
    misses = 0
    last_errors = {}
    while not _shutdown.is_set():
        found = None
        for node_id in NODES:
            try:
                r = stub(node_id).GetStatus(node_pb2.StatusRequest(), timeout=1.5)
                if r.raft_leader:
                    found = node_id
                    break
            except grpc.RpcError as exc:
                last_errors[node_id] = "%s: %s" % (exc.code(), str(exc.details())[:120])
                continue

        if found != _leader:
            log.info("leader: %s -> %s", _leader, found)
        _leader = found

        if found is None:
            misses += 1
            # Escalate rather than repeat: quiet for one blip, then insistent.
            if misses in (2, 5) or misses % 15 == 0:
                log.warning(
                    "NO LEADER after %d polls — ALL WRITES ARE BEING SKIPPED. "
                    "Last per-node errors: %s",
                    misses, last_errors or "none (every node answered, none claimed leadership)",
                )
        else:
            if misses:
                log.info("leader recovered after %d missed polls", misses)
            misses = 0
            last_errors.clear()

        _shutdown.wait(LEADER_REFRESH_S)


def worker(idx):
    seq = 0
    while not _shutdown.is_set():
        with _lock:
            rps = _target_rps
        delay = (WORKERS / rps) if rps > 0 else 1.0

        is_write = random.random() < WRITE_PCT
        target = _leader if is_write else pick_read_target()

        if target is None:
            _shutdown.wait(delay)
            continue

        seq += 1
        key = "k%d" % (seq % 50)
        with _lock:
            _stats["sent"] += 1
        try:
            if is_write:
                r = stub(target).SubmitWrite(
                    node_pb2.WriteRequest(key=key, value="w%d-%d" % (idx, seq)),
                    timeout=REQUEST_TIMEOUT_S,
                )
                ok = r.committed
            else:
                stub(target).SubmitRead(node_pb2.ReadRequest(key=key),
                                        timeout=REQUEST_TIMEOUT_S)
                ok = True
            with _lock:
                _stats["ok" if ok else "failed"] += 1
        except grpc.RpcError as exc:
            key_name = ("timed_out"
                        if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                        else "failed")
            with _lock:
                _stats[key_name] += 1

        _shutdown.wait(delay)


class LoadGenServicer(loadgen_pb2_grpc.LoadGenServiceServicer):
    def SetWeights(self, request, context):
        global _weights
        incoming = dict(request.weights)
        unknown = [k for k in incoming if k not in NODES]
        if unknown:
            return loadgen_pb2.WeightReply(ok=False, detail="unknown nodes: %s" % unknown)
        with _lock:
            _weights.update(incoming)
            snapshot = dict(_weights)
        log.info("weights set: %s", snapshot)
        return loadgen_pb2.WeightReply(ok=True, detail=str(snapshot))

    def GetWeights(self, request, context):
        with _lock:
            return loadgen_pb2.WeightTable(weights=dict(_weights))

    def SetRate(self, request, context):
        global _target_rps
        with _lock:
            _target_rps = max(0.0, request.rps)
        log.info("target rps -> %.1f", _target_rps)
        return loadgen_pb2.WeightReply(ok=True, detail="rps=%.1f" % _target_rps)

    def GetStats(self, request, context):
        with _lock:
            s = dict(_stats)
            rps = _target_rps
        return loadgen_pb2.StatsReply(
            target_rps=rps, leader=_leader or "",
            sent=s["sent"], ok=s["ok"], failed=s["failed"], timed_out=s["timed_out"],
        )


def _handle_signal(signum, _frame):
    log.info("signal %d, shutting down", signum)
    _shutdown.set()


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if not NODES:
        log.error("LOADGEN_NODES is empty; nothing to drive")
        sys.exit(1)

    log.info("nodes=%s rps=%.1f write_pct=%.2f workers=%d",
             NODES, TARGET_RPS, WRITE_PCT, WORKERS)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    loadgen_pb2_grpc.add_LoadGenServiceServicer_to_server(LoadGenServicer(), server)
    server.add_insecure_port("[::]:%d" % LISTEN_PORT)
    server.start()
    log.info("control plane on :%d", LISTEN_PORT)

    threading.Thread(target=refresh_leader, name="leader", daemon=True).start()
    # Give the leader poll one cycle before firing writes at nobody.
    time.sleep(LEADER_REFRESH_S)

    for i in range(WORKERS):
        threading.Thread(target=worker, args=(i,), name="w%d" % i, daemon=True).start()

    _shutdown.wait()
    server.stop(grace=2.0).wait(5.0)
    log.info("stopped: %s", _stats)


if __name__ == "__main__":
    main()
