"""Cluster node — real Raft + gRPC server + 1 Hz health publisher.

PHASE 2 SCOPE (plan.md §7): pysyncobj provides genuine leader election and log
replication. Voters (node-1..3) join with a self-address; the serving tier
(node-4..N) joins with selfNode=None, which registers it as a readonly member
that receives replication but never votes (plan.md §0.3).

`role` is now DERIVED from actual leadership, not configured: a voter is
Primary iff pysyncobj elected it, else Replica. The serving tier keeps its
configured Worker/Gateway role. Nothing here fakes a signal it does not have.
"""

import functools
import logging
import os
import signal
import sys
import threading
import time
import uuid
from concurrent import futures

import grpc
import psutil
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplDict

sys.path.insert(0, "/app")

from common.kafka_io import JsonProducer, wait_for_kafka  # noqa: E402
from common.schemas import (  # noqa: E402
    STATUS_HEALTHY,
    TOPIC_HEALTH,
    HealthMessage,
)
from node import behaviors, health as health_mod, raft_status  # noqa: E402
from node.reqwindow import FAIL, OK, TIMEOUT, RequestWindow  # noqa: E402
from node.peer_probe import PeerProbe  # noqa: E402
from node.rssbaseline import RssBaseline  # noqa: E402

import node_pb2  # noqa: E402
import node_pb2_grpc  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("node")

NODE_ID = os.getenv("NODE_ID", "node-0")
NODE_ROLE = os.getenv("NODE_ROLE", "Worker")
RAFT_MODE = os.getenv("RAFT_MODE", "readonly")     # voter | readonly
NODE_PEERS = [p for p in os.getenv("NODE_PEERS", "").split(",") if p]
GRPC_PORT = int(os.getenv("GRPC_PORT", "8000"))
RAFT_PORT = int(os.getenv("RAFT_PORT", "9000"))
HEALTH_INTERVAL = float(os.getenv("HEALTH_INTERVAL", "1.0"))

IS_VOTER = RAFT_MODE == "voter"
SELF_RAFT_ADDR = "%s:%d" % (NODE_ID, RAFT_PORT)

# plan.md §4.1b / network.py:707 — a replica serving a read while this far
# behind is a stale read.
READ_STALE_LAG_THRESHOLD = 2
WRITE_TIMEOUT_S = 5.0

STATUS_ISOLATED = "isolated"

# plan.md §4.1a — boot_id is the ONLY restart proof. Generated once, here.
# uptime is a duration and can never answer "is this the same process".
BOOT_ID = str(uuid.uuid4())
STARTED_AT = time.time()

_shutdown = threading.Event()
_proc = psutil.Process()


class NodeState:
    """Mutable per-node state. Phase 1 holds only what actually exists."""

    def __init__(self):
        self.lock = threading.Lock()
        self.role = NODE_ROLE
        self.status = STATUS_HEALTHY
        self.health = 100.0
        self.queue_length = 0
        self.shedding = False
        self.isolated = False
        # Consensus fields stay zero until Phase 2 wires pysyncobj.
        self.raft_leader = False
        self.raft_term = 0
        self.commit_index = 0
        self.apply_lag = 0
        # Sampled ONCE per publish cycle by the health thread; every other
        # reader takes this cached value. See _sample_cpu().
        self.cpu_pct = 0.0
        self.rss_mb = 0.0

    def snapshot(self):
        with self.lock:
            return dict(
                role=self.role,
                status=self.status,
                health=self.health,
                queue_length=self.queue_length,
                shedding=self.shedding,
                isolated=self.isolated,
                raft_leader=self.raft_leader,
                raft_term=self.raft_term,
                commit_index=self.commit_index,
                apply_lag=self.apply_lag,
                cpu_pct=self.cpu_pct,
                rss_mb=self.rss_mb,
            )


STATE = NodeState()

# ---------------------------------------------------------------------- raft

SYNC = None      # SyncObj instance
STORE = None     # ReplDict replicated through Raft

# Rolling 1s window of served requests -> req_ok/fail/timeout + p50/p99.
REQUESTS = RequestWindow(window=1.0)

# Median RSS over the 30-60s post-start window (§5.5). Stays None while
# warming, which is how the leak rule knows to abstain.
RSS_BASELINE = RssBaseline(STARTED_AT)

# Every node in the cluster, for the RTT matrix. Probing ALL peers (not just
# voters) is what gives the serving tier a latency signal at all — the leader's
# view of readonly nodes is anonymous (§4.6a), but a node naming its own peers
# is not.
ALL_NODES = [n for n in os.getenv("ALL_NODES", "").split(",") if n]
PROBE = None


def init_raft():
    """Join the Raft group as a voter, or the serving tier as a readonly member.

    A readonly member is created by passing selfNode=None (pysyncobj
    transport.py:360 treats a node with no self-address as readonly). It
    receives full log replication but never votes, so quorum arithmetic is
    unchanged and the serving tier stays sheddable — plan.md §0.3.
    """
    global SYNC, STORE

    conf = SyncObjConf(
        autoTick=True,
        appendEntriesUseBatch=True,
        connectionRetryTime=1.0,
        # Defaults (0.4 / 1.4) left alone: Phase 8's forced step-down sizes its
        # isolation window at 2x raftMaxTimeout, so changing these moves that.
        onStateChanged=_on_state_changed,
    )
    STORE = ReplDict()

    if IS_VOTER:
        partners = [p for p in NODE_PEERS if p != SELF_RAFT_ADDR]
        log.info("raft: voter %s partners=%s", SELF_RAFT_ADDR, partners)
        SYNC = SyncObj(SELF_RAFT_ADDR, partners, conf=conf, consumers=[STORE])
    else:
        log.info("raft: readonly member, voters=%s", NODE_PEERS)
        SYNC = SyncObj(None, list(NODE_PEERS), conf=conf, consumers=[STORE])


def _on_state_changed(old_state, new_state):
    # Leadership transitions are the single most useful line in the logs when a
    # failover gate fails; surface them rather than leaving them to telemetry.
    log.info("raft: state %s -> %s", old_state, new_state)


def is_leader() -> bool:
    return bool(SYNC and SYNC._isLeader())


def derived_role() -> str:
    """Role follows reality, not configuration.

    A voter is Primary iff pysyncobj actually elected it. The serving tier
    keeps its configured Worker/Gateway role — it is not part of consensus, so
    leadership says nothing about it.
    """
    if not IS_VOTER:
        return NODE_ROLE
    return "Primary" if is_leader() else "Replica"


def _sample_cpu():
    """Sample CPU/RSS into STATE. MUST be called from exactly one thread.

    psutil's cpu_percent(interval=None) measures since the last call *on this
    Process object*, so two interleaved callers corrupt each other: whichever
    calls second reads a near-zero window, and the other's next reading covers
    a randomly truncated one. That is what produced a 53% reading on an idle
    node at startup.

    It would get materially worse from Phase 6: §6.2a has the executor call
    GetStatus before *every* action, so an independent sample there would
    corrupt published load precisely during recovery — the moment the telemetry
    matters most, and the moment the cpu_overload rule is reading it.

    So: the health thread is the sole sampler, and GetStatus reads the cache.
    """
    with STATE.lock:
        STATE.cpu_pct = _proc.cpu_percent(interval=None)
        STATE.rss_mb = _proc.memory_info().rss / (1024 * 1024)
        rss = STATE.rss_mb
    RSS_BASELINE.observe(rss)


def build_health_message():
    _sample_cpu()               # sole sampling point, once per cycle
    snap = STATE.snapshot()
    now = time.time()
    cpu = snap["cpu_pct"]

    leader = is_leader()
    raft = raft_status.project(SYNC.getStatus(), leader) if SYNC else {}
    reqs = REQUESTS.snapshot()

    # Compute the real score (§4.5). This was hardcoded to 100.0 for Phases
    # 1-8, which fed a constant to every health-keyed branch in the reused
    # decision code — see node/health.py for what that silently disabled.
    hp, hstatus, hwhy = health_mod.compute(
        apply_lag=raft.get("apply_lag", 0),
        req_ok=reqs["req_ok"], req_fail=reqs["req_fail"],
        req_timeout=reqs["req_timeout"],
        rss_mb=snap["rss_mb"], rss_baseline_mb=RSS_BASELINE.value,
    )
    with STATE.lock:
        STATE.health = hp
        STATE.status = hstatus
    if hwhy and hstatus != "healthy":
        log.info("health %.0f (%s): %s", hp, hstatus, "; ".join(hwhy))

    # feedback.evaluate_action_result judges isolate_node by
    # `status in ["failed", "isolated"]` (feedback.py:97-100). Reporting
    # "healthy" with a separate isolated=True flag made every isolation read
    # as a FAILED action and trigger a pointless follow-up. Surface the state
    # in the field the reused criteria actually read.
    status = "isolated" if snap["isolated"] else hstatus

    return HealthMessage(
        node_id=NODE_ID,
        role=derived_role(),
        status=status,
        health=hp,
        load=cpu,
        ts=now,
        boot_id=BOOT_ID,
        started_at=STARTED_AT,
        uptime=now - STARTED_AT,
        queue_length=snap["queue_length"],
        raft_leader=raft.get("raft_leader", False),
        raft_term=raft.get("raft_term", 0),
        has_quorum=raft.get("has_quorum", True),
        commit_index=raft.get("commit_index", 0),
        # apply_lag is self-observed and identical in shape on voters and the
        # serving tier (§4.6a). replica_lag aliases it inside HealthMessage.
        apply_lag=raft.get("apply_lag", 0),
        match_lag=raft.get("match_lag"),           # leader only, voters only
        leader_commit_idx=raft.get("leader_commit_idx"),
        peers_reachable=raft.get("peers_reachable"),
        peer_rtt_ms=(PROBE.current() if PROBE else None),
        peer_rtt_baseline_ms=(PROBE.baseline() if PROBE else None),
        # Windowed over 1s, never cumulative (§4.1 / §8.3). p50/p99 are None
        # below the sample floor so the §5.5 latency rule abstains rather than
        # firing on a percentile computed from three points.
        req_ok=reqs["req_ok"],
        req_fail=reqs["req_fail"],
        req_timeout=reqs["req_timeout"],
        req_p50_ms=reqs["req_p50_ms"],
        req_p99_ms=reqs["req_p99_ms"],
        cpu_pct=cpu,
        rss_mb=snap["rss_mb"],
        # Median over the 30-60s post-start window; None while still warming,
        # so the leak rule abstains rather than comparing against a
        # pre-cache-fill reading that would make warm-up look like a leak.
        rss_baseline_mb=RSS_BASELINE.value,
        shedding=snap["shedding"],
        isolated=snap["isolated"],
    )


def health_publisher():
    producer = JsonProducer(client_id=NODE_ID)
    log.info("health publisher started, interval=%.1fs -> %s", HEALTH_INTERVAL, TOPIC_HEALTH)
    next_tick = time.time()
    while not _shutdown.is_set():
        try:
            msg = build_health_message()
            producer.send(TOPIC_HEALTH, msg.to_dict(), key=NODE_ID)
        except Exception as exc:  # never let telemetry kill the node
            log.warning("health publish failed: %s", exc)
        next_tick += HEALTH_INTERVAL
        _shutdown.wait(max(0.0, next_tick - time.time()))
    producer.flush(2.0)
    log.info("health publisher stopped")


_inflight = 0
_inflight_lock = threading.Lock()


def tracked(handler):
    """Count concurrently-served requests -> queue_length.

    Real queue pressure, not a modelled number: it rises when the node cannot
    keep up, which is exactly the signal cpu_overload and load shedding read.
    A decorator rather than a `with` block so the handler bodies stay flat and
    every early return is covered.
    """

    @functools.wraps(handler)
    def wrapper(self, request, context):
        global _inflight
        with _inflight_lock:
            _inflight += 1
            STATE.queue_length = _inflight
        try:
            return handler(self, request, context)
        finally:
            with _inflight_lock:
                _inflight = max(0, _inflight - 1)
                STATE.queue_length = _inflight

    return wrapper


def _record(t0, context, outcome):
    """Record one served request into the rolling window.

    A client that has already given up is counted as a TIMEOUT rather than OK:
    the node finished, but nobody was listening, which is the honest outcome
    from the cluster's point of view. context.is_active() is what makes that
    observable server-side.
    """
    latency_ms = (time.time() - t0) * 1000.0
    if outcome == OK and not context.is_active():
        outcome = TIMEOUT
    REQUESTS.record(latency_ms, outcome)


class NodeServicer(node_pb2_grpc.NodeServiceServicer):
    def GetStatus(self, request, context):
        snap = STATE.snapshot()
        now = time.time()
        leader = is_leader()
        raft = raft_status.project(SYNC.getStatus(), leader) if SYNC else {}
        return node_pb2.StatusReply(
            node_id=NODE_ID,
            role=derived_role(),
            status=snap["status"],
            health=snap["health"],
            # Cached, never freshly sampled here — see _sample_cpu(). The
            # executor calls this before every action from Phase 6 on.
            load=snap["cpu_pct"],
            queue_length=snap["queue_length"],
            apply_lag=raft.get("apply_lag", 0),
            raft_leader=leader,
            commit_index=raft.get("commit_index", 0),
            raft_term=raft.get("raft_term", 0),
        has_quorum=raft.get("has_quorum", True),
            boot_id=BOOT_ID,
            started_at=STARTED_AT,
            uptime=now - STARTED_AT,
            shedding=snap["shedding"],
            isolated=snap["isolated"],
        )

    @tracked
    def SubmitWrite(self, request, context):
        if STATE.shedding:
            _record(time.time(), context, FAIL)
            context.abort(grpc.StatusCode.UNAVAILABLE, "shedding")
            return
        """Only the leader can commit. `committed` is never optimistic — it is
        set solely on a synchronous Raft commit, so a write that lost quorum
        reports failure rather than a lie the evaluator would later count.

        Write latency is network-sensitive: STORE.set(sync=True) blocks on
        quorum acknowledgement, so it traverses the wire to the peers. That is
        what makes it able to see `tc netem` delay at all (see SubmitRead)."""
        t0 = time.time()
        if not is_leader():
            leader = raft_status.leader_id(SYNC.getStatus()) if SYNC else None
            _record(t0, context, FAIL)
            return node_pb2.WriteReply(
                committed=False,
                error="not_leader:%s" % (leader or "unknown"),
            )
        try:
            STORE.set(request.key, request.value, sync=True, timeout=WRITE_TIMEOUT_S)
        except Exception as exc:
            # Lost leadership mid-write, or no quorum. Both are real outcomes.
            _record(t0, context, FAIL)
            return node_pb2.WriteReply(committed=False, error="commit_failed:%s" % exc)

        st = SYNC.getStatus()
        _record(t0, context, OK)
        return node_pb2.WriteReply(
            committed=True,
            commit_index=int(st.get("commit_idx", 0)),
        )

    @tracked
    def SubmitRead(self, request, context):
        if STATE.shedding:
            _record(time.time(), context, FAIL)
            context.abort(grpc.StatusCode.UNAVAILABLE, "shedding")
            return
        """Local read. Staleness is measured, not assumed: apply_lag is this
        node's own leader_commit_idx - last_applied (§4.6a), so the serving
        tier reports genuine staleness rather than a modelled guess.

        NOTE for Phase 4: a read is served entirely from local state, so its
        latency does NOT traverse the network and will not reflect `tc netem`
        path delay. Only writes (quorum round-trip) will. See plan.md §5.2's
        latency rule caveat.
        """
        t0 = time.time()
        if STORE is None:
            _record(t0, context, FAIL)
            return node_pb2.ReadReply(error="raft_not_ready")
        st = SYNC.getStatus()
        lag = raft_status.apply_lag(st)
        value = STORE.get(request.key)
        _record(t0, context, OK)
        return node_pb2.ReadReply(
            value="" if value is None else str(value),
            stale=lag >= READ_STALE_LAG_THRESHOLD,
            lag=lag,
        )

    def AdminAction(self, request, context):
        """Phase 4 implements the self-inflicted faults only.

        ISOLATE/REJOIN (Phase 6) and ENTER_SHED/EXIT_SHED (Phase 8) stay
        UNIMPLEMENTED rather than returning a misleading ok=True — a verb that
        silently does nothing is worse than one that refuses outright.
        """
        verb = request.verb
        if verb == node_pb2.START_LEAK:
            rate = float(request.arg) if request.arg else 5.0
            ok, detail = behaviors.LEAK.start(rate)
        elif verb == node_pb2.STOP_LEAK:
            ok, detail = behaviors.LEAK.stop()
        elif verb == node_pb2.START_CPU_BURN:
            workers = int(request.arg) if request.arg else 2
            ok, detail = behaviors.BURN.start(workers)
        elif verb == node_pb2.STOP_CPU_BURN:
            ok, detail = behaviors.BURN.stop()
        elif verb == node_pb2.ISOLATE:
            # The node marks itself isolated; the iptables rules are applied by
            # the executor from outside. NOT restart-durable — a restart
            # recreates the namespace and clears this flag (§6.2a).
            with STATE.lock:
                STATE.isolated = True
            ok, detail = True, "marked isolated"
        elif verb == node_pb2.REJOIN:
            with STATE.lock:
                STATE.isolated = False
            ok, detail = True, "marked rejoined"
        elif verb == node_pb2.ENTER_SHED:
            # Shed is NOT isolation. The node stops serving CLIENT requests but
            # keeps full Raft participation — it still votes, still replicates,
            # still counts toward quorum. That is the whole point: shedding
            # relieves load without touching consensus, which is why
            # _rank_shed_candidates (llm_agent.py:326) is allowed to shed only
            # the non-voting tier and why quorum maths is unaffected.
            with STATE.lock:
                STATE.shedding = True
            ok, detail = True, "shedding client requests; Raft participation intact"
        elif verb == node_pb2.EXIT_SHED:
            with STATE.lock:
                STATE.shedding = False
            ok, detail = True, "accepting client requests again"
        else:
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "verb %s lands in Phase 6/8" % node_pb2.AdminVerb.Name(verb),
            )
            return
        return node_pb2.AdminReply(ok=ok, detail=detail)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    node_pb2_grpc.add_NodeServiceServicer_to_server(NodeServicer(), server)
    server.add_insecure_port("[::]:%d" % GRPC_PORT)
    server.start()
    log.info("gRPC listening on :%d", GRPC_PORT)
    return server


def _handle_signal(signum, _frame):
    log.info("signal %d received, shutting down", signum)
    _shutdown.set()


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "starting %s role=%s raft_mode=%s peers=%s boot_id=%s",
        NODE_ID, NODE_ROLE, RAFT_MODE, NODE_PEERS, BOOT_ID,
    )

    _sample_cpu()  # prime psutil so the first published sample is a real delta

    if not wait_for_kafka():
        log.error("kafka unreachable; exiting rather than restart-looping")
        sys.exit(1)

    init_raft()
    server = serve()

    global PROBE
    peers = ALL_NODES or [p.rsplit(":", 1)[0] for p in NODE_PEERS]
    PROBE = PeerProbe(NODE_ID, peers, GRPC_PORT, node_pb2_grpc.NodeServiceStub)
    PROBE.bind_request(node_pb2.StatusRequest())
    PROBE.start()
    pub = threading.Thread(target=health_publisher, name="health", daemon=True)
    pub.start()

    _shutdown.wait()
    server.stop(grace=2.0).wait(5.0)
    pub.join(timeout=5.0)
    if PROBE is not None:
        PROBE.stop()
    if SYNC is not None:
        SYNC.destroy()
    log.info("stopped")


if __name__ == "__main__":
    main()
