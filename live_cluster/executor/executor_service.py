"""Executor — turns intents into real actions. plan.md §6, Phase 6.

Consumes cluster.actions, validates against a FRESH GetStatus (§6.2a), applies
via the Docker API / gRPC, and publishes every state transition to
cluster.action_results.

`cluster.actions` is an INTENT. `cluster.action_results` is the outcome.
Nothing else may be read as "the action happened".

SECURITY: this process mounts /var/run/docker.sock, which is root-equivalent
control of the host (plan.md §11). Localhost development only.
"""

import json
import logging
import os
import signal
import sys
import threading
import time

import docker
import grpc

sys.path.insert(0, "/app")

from common.kafka_io import (  # noqa: E402
    JsonProducer, ensure_topics, make_consumer, wait_for_kafka,
)
from common.schemas import TOPIC_ACTION_RESULTS, TOPIC_ACTIONS  # noqa: E402
from executor import validation as V  # noqa: E402
from executor.weights import WeightClient  # noqa: E402
from executor.transition_log import (  # noqa: E402
    ACCEPTED, APPLYING, COMPLETED, FAILED, RECONCILED, REJECTED, TransitionLog,
)

import loadgen_pb2  # noqa: E402
import loadgen_pb2_grpc  # noqa: E402
import node_pb2  # noqa: E402
import node_pb2_grpc  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("executor")

GROUP_ID = os.getenv("EXECUTOR_GROUP_ID", "executor")
NODE_PORT = int(os.getenv("NODE_GRPC_PORT", "8000"))
LOG_PATH = os.getenv("EXECUTOR_STATE", "/state/transitions.jsonl")
RESTART_TIMEOUT_S = int(os.getenv("RESTART_TIMEOUT_S", "30"))
LOADGEN_HOST = os.getenv("LOADGEN_HOST", "load_generator")
LOADGEN_PORT = int(os.getenv("LOADGEN_PORT", "9500"))
# §0.2 measured raftMaxTimeout=1.4s. Phase 8 sizes the forced step-down
# isolation at ~2x that, then heals.
STEPDOWN_ISOLATION_S = float(os.getenv("STEPDOWN_ISOLATION_S", "2.8"))

_shutdown = threading.Event()
LOG = TransitionLog(LOG_PATH)
PRODUCER = JsonProducer(client_id="executor")
DOCKER = docker.from_env()
WEIGHTS = WeightClient(LOADGEN_HOST, LOADGEN_PORT, loadgen_pb2, loadgen_pb2_grpc)


def publish(decision_id, incident_id, state, detail="", rejected_reason=None):
    PRODUCER.send(TOPIC_ACTION_RESULTS, {
        "decision_id": decision_id, "incident_id": incident_id,
        "state": state, "detail": detail, "ts": time.time(),
        "rejected_reason": rejected_reason,
    }, key=decision_id)


def get_status(target):
    """Fresh read — the actual safety boundary (§6.2). Returns None when the
    node cannot be reached, which is itself meaningful and action-dependent."""
    try:
        ch = grpc.insecure_channel("%s:%d" % (target, NODE_PORT))
        st = node_pb2_grpc.NodeServiceStub(ch).GetStatus(
            node_pb2.StatusRequest(), timeout=3.0)
        return {
            "node_id": st.node_id, "role": st.role, "status": st.status,
            "boot_id": st.boot_id, "started_at": st.started_at,
            "uptime": st.uptime, "has_quorum": st.has_quorum,
            "raft_leader": st.raft_leader,
        }
    except grpc.RpcError:
        return None


def admin(target, verb, arg=""):
    ch = grpc.insecure_channel("%s:%d" % (target, NODE_PORT))
    r = node_pb2_grpc.NodeServiceStub(ch).AdminAction(
        node_pb2.AdminRequest(verb=verb, arg=str(arg)), timeout=10.0)
    return r.ok, r.detail


# ----------------------------------------------------------------- actions


def do_restart(target, intent):
    """ENSURE_RUNNING semantics: the boot_id must CHANGE, which is what proves
    the restart happened (§6.4). A duration would prove nothing."""
    before = (get_status(target) or {}).get("boot_id")
    container = DOCKER.containers.get(target)
    container.restart(timeout=10)

    deadline = time.time() + RESTART_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(1.5)
        st = get_status(target)
        if st and st.get("boot_id") and st["boot_id"] != before:
            return True, "boot_id %s -> %s" % (
                (before or "?")[:8], st["boot_id"][:8])
    return False, "restarted but no new boot_id within %ds" % RESTART_TIMEOUT_S


def do_isolate(target, intent):
    """AdminAction(ISOLATE) plus iptables DROP against every peer.

    NOT restart-durable: a container restart recreates the network namespace
    and the rules vanish (§6.2a). The agent re-observes and re-decides.
    """
    ok, detail = admin(target, node_pb2.ISOLATE)
    peers = [c.name for c in DOCKER.containers.list(
        filters={"label": "live_cluster.role=node"}) if c.name != target]
    cut = 0
    for peer in peers:
        try:
            ip = DOCKER.containers.get(peer).attrs[
                "NetworkSettings"]["Networks"]["live_cluster"]["IPAddress"]
        except (KeyError, docker.errors.NotFound):
            continue
        c = DOCKER.containers.get(target)
        c.exec_run("iptables -A INPUT -s %s -j DROP" % ip)
        c.exec_run("iptables -A OUTPUT -d %s -j DROP" % ip)
        cut += 1
    return True, "%s; cut from %d peers" % (detail if ok else "admin refused", cut)


def do_monitor(target, intent):
    return True, "monitoring, no action taken"


def do_reroute(target, intent):
    return WEIGHTS.reroute_away_from(target, fraction=0.5)


def do_rebalance(target, intent):
    """Flatten across everything still serving. Shed nodes are excluded — they
    were taken out of rotation deliberately, so handing them traffic back
    would undo a decision nobody revisited."""
    shed = set()
    for name in _node_names():
        st = get_status(name)
        if st is None:
            shed.add(name)
    return WEIGHTS.rebalance(exclude=shed)


def do_load_shed(target, intent):
    ok, detail = admin(target, node_pb2.ENTER_SHED)
    wok, wdetail = WEIGHTS.zero(target)
    return (ok and wok), "%s; %s" % (detail, wdetail)


def do_readmit(target, intent):
    ok, detail = admin(target, node_pb2.EXIT_SHED)
    wok, wdetail = WEIGHTS.restore(target)
    return (ok and wok), "%s; %s" % (detail, wdetail)


def do_promote_replica(target, intent):
    """FORCED STEP-DOWN. plan.md §0.2.

    pysyncobj 0.3.12 exposes no leadership transfer (Gate 0.2, verified), so
    leadership cannot be handed to a chosen node. What IS possible is making
    the sitting leader stop being leader: isolate it for ~2x raftMaxTimeout and
    the remaining voters elect someone.

    YOU DO NOT CHOOSE THE WINNER. The intent names a target; the cluster
    decides. So this records the intent, the pre-state and the ACTUAL elected
    node separately, and the result is scored against what happened — never
    against what was asked for.
    """
    before = _leader_name()
    if before is None:
        return False, "no leader observable; nothing to step down"

    peers = [n for n in _node_names() if n != before]
    ips = []
    for peer in peers:
        try:
            ips.append(DOCKER.containers.get(peer).attrs[
                "NetworkSettings"]["Networks"]["live_cluster"]["IPAddress"])
        except (KeyError, docker.errors.NotFound):
            continue

    c = DOCKER.containers.get(before)
    for ip in ips:
        c.exec_run("iptables -A INPUT -s %s -j DROP" % ip)
        c.exec_run("iptables -A OUTPUT -d %s -j DROP" % ip)
    time.sleep(STEPDOWN_ISOLATION_S)
    for ip in ips:
        c.exec_run("iptables -D INPUT -s %s -j DROP" % ip)
        c.exec_run("iptables -D OUTPUT -d %s -j DROP" % ip)

    time.sleep(3.0)
    after = _leader_name()
    moved = after is not None and after != before
    return moved, ("leader %s -> %s (intent named %s; the cluster chose, "
                   "not us)" % (before, after, target))


def _node_names():
    return sorted(c.name for c in DOCKER.containers.list(
        filters={"label": "live_cluster.role=node"}))


def _leader_name():
    for name in _node_names():
        st = get_status(name)
        if st and st.get("raft_leader"):
            return name
    return None


ACTIONS = {
    "restart_node": do_restart,
    "ENSURE_RUNNING": do_restart,
    "isolate_node": do_isolate,
    "monitor_only": do_monitor,
    # Phase 8
    "reroute_traffic": do_reroute,
    "rebalance_load": do_rebalance,
    "load_shed": do_load_shed,
    "load_shed_readmit": do_readmit,
    "promote_replica": do_promote_replica,
}


# -------------------------------------------------------------- the loop


def handle(intent):
    did = intent.get("decision_id")
    iid = intent.get("incident_id")
    action = intent.get("action")
    target = intent.get("target")

    wt = None
    if action in V.ACTS_ON_WEIGHTS:
        try:
            wt = set(WEIGHTS.get().keys())
        except grpc.RpcError:
            wt = None          # unknown, not empty — do not reject on a guess
    verdict = V.validate(intent, LOG, get_status(target), weight_table_targets=wt)
    if not verdict.accepted:
        LOG.append(did, REJECTED, target=target, action=action,
                   decided_at=intent.get("decided_at"),
                   reason=verdict.decision)
        publish(did, iid, REJECTED, verdict.reason, verdict.decision)
        log.info("REJECTED %s %s on %s — %s (%s)", did, action, target,
                 verdict.decision, verdict.reason)
        return

    fn = ACTIONS.get(action)
    if fn is None:
        LOG.append(did, REJECTED, target=target, action=action,
                   decided_at=intent.get("decided_at"), reason="unsupported")
        publish(did, iid, REJECTED, "action not implemented in Phase 6",
                "unsupported")
        log.info("REJECTED %s unsupported action %s", did, action)
        return

    # Write-ahead: both records durable BEFORE the side effect. A buffered
    # write lost in a crash would void the whole guarantee (§6.3).
    LOG.append(did, ACCEPTED, target=target, action=action,
               decided_at=intent.get("decided_at"))
    publish(did, iid, ACCEPTED, verdict.reason)
    LOG.append(did, APPLYING, target=target, action=action,
               decided_at=intent.get("decided_at"))
    # Publish it too, not just log it. §4.2 lists `applying` as a real state of
    # cluster.action_results, and the evaluator reads per-stage timing (queue
    # delay vs apply duration) off the TOPIC — it has no access to the
    # executor's local file.
    publish(did, iid, APPLYING, "applying %s to %s" % (action, target))
    log.info("APPLYING %s %s on %s", did, action, target)

    try:
        ok, detail = fn(target, intent)
    except Exception as exc:
        LOG.append(did, FAILED, target=target, action=action,
                   decided_at=intent.get("decided_at"), detail=str(exc))
        publish(did, iid, FAILED, "%s: %s" % (type(exc).__name__, exc))
        log.exception("FAILED %s %s on %s", did, action, target)
        return

    state = COMPLETED if ok else FAILED
    LOG.append(did, state, target=target, action=action,
               decided_at=intent.get("decided_at"), detail=detail)
    publish(did, iid, state, detail)
    log.info("%s %s %s on %s — %s", state.upper(), did, action, target, detail)


def reconcile_orphans():
    """Any decision whose latest state is `applying` means we died mid-apply.

    `reconciled` is a distinct state precisely so the ambiguity is VISIBLE in
    the audit trail rather than papered over by a silent duplicate rejection.
    """
    for did in LOG.orphans():
        rec = LOG.records().get(did, {})
        target = rec.get("target")
        st = get_status(target) if target else None
        finding = ("target reachable, boot_id=%s" % (st.get("boot_id", "?")[:8])
                   if st else "target unreachable")
        LOG.append(did, RECONCILED, target=target, action=rec.get("action"),
                   decided_at=rec.get("decided_at"), detail=finding)
        publish(did, rec.get("incident_id", ""), RECONCILED, finding)
        log.warning("RECONCILED orphan %s (%s on %s) — %s",
                    did, rec.get("action"), target, finding)


def _handle_signal(signum, _frame):
    log.info("signal %d, shutting down", signum)
    _shutdown.set()


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.warning("executor has root-equivalent host control via docker.sock "
                "(plan.md §11) — localhost development only")

    if not wait_for_kafka():
        log.error("kafka unreachable; exiting")
        sys.exit(1)

    ensure_topics()
    reconcile_orphans()

    # EARLIEST, deliberately. An executor that starts at `latest` silently
    # skips any intent produced before its partition assignment resolved —
    # which is how the first Phase 6 gate lost its only intent.
    #
    # Replaying from the beginning is safe here *by construction*: decision_ids
    # are deterministic (§6.1) and the transition log rejects anything already
    # seen (§6.3). That machinery exists precisely so at-least-once redelivery
    # is harmless, so using it is the point rather than a risk. An intent
    # dropped on the floor is far worse than one offered twice.
    consumer = make_consumer(GROUP_ID, [TOPIC_ACTIONS], earliest=True)
    log.info("consuming %s", TOPIC_ACTIONS)
    try:
        while not _shutdown.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                intent = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            try:
                handle(intent)
            except Exception:
                log.exception("handler crashed on %s", intent.get("decision_id"))
            # Commit only AFTER the transition log is durable.
            consumer.commit(msg)
    finally:
        consumer.close()
        PRODUCER.flush(3.0)
    log.info("stopped")


if __name__ == "__main__":
    main()
