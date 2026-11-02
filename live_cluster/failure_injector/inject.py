#!/usr/bin/env python3
"""Real chaos against real containers. plan.md Phase 4.

    python3 failure_injector/inject.py crash     --target node-2
    python3 failure_injector/inject.py partition --a node-1 --b node-2 --duration 10
    python3 failure_injector/inject.py latency   --target node-3 --ms 300 --duration 10
    python3 failure_injector/inject.py leak      --target node-4 --rate-mb 5
    python3 failure_injector/inject.py cpu       --target node-5 --workers 2

Every command publishes an `injected` ground-truth record to cluster.failures
BEFORE acting, carrying a fresh gt_id.

GROUND TRUTH IS EVALUATOR-ONLY (plan.md §1). The recovery agent must never
subscribe to cluster.failures — feeding it labels would collapse the thesis
into "given a labeled failure, pick an action", which the simulator already
answers. This tool writes the answer key; nothing under recovery_agent/ may
read it.

Resolution semantics (plan.md §4.2): only `partition` and `latency` are
self-healing on a timer, so only those publish a `resolved` record with
resolved_by=injector. crash/leak/cpu are undone by the recovery system under
test, so their resolution is DERIVED by the evaluator from cluster.health.

Deviation from the plan's layout: this runs on the HOST via the `docker` CLI
rather than in a container with the Docker SDK. Same effect, and it avoids
mounting /var/run/docker.sock two phases before the executor needs it (§11).

Host-side, Python 3.9 compatible, no third-party deps.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE_DIR = os.path.dirname(HERE)
NODE_LABEL = "live_cluster.role=node"
NETWORK = "live_cluster"
TOPIC = "cluster.failures"


def sh(cmd, timeout=120, check=False):
    r = subprocess.run(cmd, shell=True, cwd=COMPOSE_DIR, capture_output=True,
                       text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit("command failed: %s\n%s" % (cmd, r.stderr.strip()))
    return r


def discover_nodes():
    """Find node containers by LABEL, never by hardcoded count — the injector
    must work against whatever N the compose file was rendered for."""
    r = sh("docker ps --filter label=%s --format '{{.Names}}'" % NODE_LABEL)
    return sorted(n.strip() for n in r.stdout.splitlines() if n.strip())


def container_ip(name):
    r = sh("docker inspect -f "
           "'{{.NetworkSettings.Networks.%s.IPAddress}}' %s" % (NETWORK, name))
    ip = r.stdout.strip()
    if not ip:
        raise SystemExit("no IP for %s on network %s" % (name, NETWORK))
    return ip


def assert_net_admin(node):
    """Fail LOUDLY if NET_ADMIN is missing.

    Without the capability iptables/tc return non-zero and the injection
    silently no-ops — the run would look like a classifier miss rather than a
    broken harness (plan.md §11).
    """
    r = sh("docker exec %s iptables -L -n" % node)
    if r.returncode != 0:
        raise SystemExit(
            "NET_ADMIN missing or iptables unavailable in %s.\n"
            "  compose must set cap_add: [NET_ADMIN]. Refusing to inject a\n"
            "  fault that would silently do nothing.\n  %s"
            % (node, r.stderr.strip()[:300])
        )


def publish(record):
    """Append a ground-truth record to cluster.failures."""
    payload = json.dumps(record, separators=(",", ":"))
    p = subprocess.run(
        "docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh "
        "--bootstrap-server localhost:9092 --topic %s" % TOPIC,
        shell=True, cwd=COMPOSE_DIR, input=payload + "\n",
        capture_output=True, text=True, timeout=60,
    )
    if p.returncode != 0:
        print("WARNING: ground truth not published: %s" % p.stderr.strip()[:200],
              file=sys.stderr)
    return record


def gt_injected(failure_type, target, duration_hint=None):
    rec = {
        "event": "injected",
        "gt_id": "gt-" + uuid.uuid4().hex[:10],
        "failure_type": failure_type,
        "target": target,
        "injected_at": time.time(),
        "duration_hint": duration_hint,
    }
    publish(rec)
    print("  ground truth: %s %s on %s" % (rec["gt_id"], failure_type, target))
    return rec


def gt_resolved(gt_id, resolved_by="injector"):
    rec = {
        "event": "resolved",
        "gt_id": gt_id,
        "resolved_at": time.time(),
        "resolved_by": resolved_by,
    }
    publish(rec)
    print("  ground truth: %s resolved by %s" % (gt_id, resolved_by))
    return rec


def admin_action(node, verb, arg=""):
    """Call the node's own AdminAction over localhost inside its container.

    Keeps the injector free of grpc/stub dependencies on the host.
    """
    script = (
        "import grpc, node_pb2, node_pb2_grpc\n"
        "st = node_pb2_grpc.NodeServiceStub(grpc.insecure_channel('localhost:8000'))\n"
        "r = st.AdminAction(node_pb2.AdminRequest(verb=node_pb2.%s, arg=%r))\n"
        "print('%%s|%%s' %% (r.ok, r.detail))\n" % (verb, str(arg))
    )
    r = sh("docker compose exec -T %s python -c \"%s\"" % (node, script.replace('"', '\\"')),
           timeout=60)
    out = (r.stdout or "").strip().splitlines()
    if not out:
        raise SystemExit("AdminAction %s on %s produced no output:\n%s"
                         % (verb, node, (r.stderr or "")[:400]))
    ok, _, detail = out[-1].partition("|")
    return ok == "True", detail


# ------------------------------------------------------------------ commands


def cmd_crash(args):
    node = args.target
    print("crash: %s" % node)
    gt_injected("crash", node)
    sh("docker kill %s" % node, check=True)
    print("  killed. NOT restarting — recovery is the system under test.")
    print("  (resolution will be DERIVED by the evaluator from cluster.health)")


def cmd_partition(args):
    a, b = args.a, args.b
    assert_net_admin(a)
    assert_net_admin(b)
    ip_a, ip_b = container_ip(a), container_ip(b)
    target = "%s<->%s" % (a, b)
    print("partition: %s (%s) <-> %s (%s)" % (a, ip_a, b, ip_b))

    rec = gt_injected("network_partition", target, args.duration)

    rules = [(a, ip_b), (b, ip_a)]
    if args.blackout:
        # Optional hard case (plan.md §5.6): also cut telemetry, collapsing
        # rules 1 and 2 into silence-vs-silence. Never gates a phase.
        kafka_ip = container_ip("kafka")
        rules += [(a, kafka_ip), (b, kafka_ip)]
        print("  --blackout: also dropping Kafka (%s)" % kafka_ip)

    for node, peer_ip in rules:
        sh("docker exec %s iptables -A INPUT -s %s -j DROP" % (node, peer_ip), check=True)
        sh("docker exec %s iptables -A OUTPUT -d %s -j DROP" % (node, peer_ip), check=True)

    print("  partitioned for %.1fs..." % args.duration)
    time.sleep(args.duration)

    for node, peer_ip in rules:
        sh("docker exec %s iptables -D INPUT -s %s -j DROP" % (node, peer_ip))
        sh("docker exec %s iptables -D OUTPUT -d %s -j DROP" % (node, peer_ip))
    print("  healed.")
    gt_resolved(rec["gt_id"], "injector")


def cmd_isolate(args):
    """Cut a node from EVERY peer — a majority cut.

    Distinct from `partition`, which cuts one link. Measurement (plan.md §5.5a)
    showed a single-link cut in a 3-voter cluster is absorbed by Raft: the node
    that can still see both sides takes leadership and the cluster returns to
    fully healthy, leaving no detectable trace. Correct behaviour, but nothing
    for a classifier to find.

    A majority cut produces the real thing: the isolated node campaigns forever
    (term climbs without bound), the leader's match_lag for it grows
    monotonically, and it loses quorum.
    """
    victim = args.target
    peers = [n for n in discover_nodes() if n != victim]
    assert_net_admin(victim)
    victim_ip = container_ip(victim)
    print("isolate: %s from %d peers (majority cut)" % (victim, len(peers)))

    rec = gt_injected("network_partition", victim, args.duration)

    cut = []
    for peer in peers:
        try:
            peer_ip = container_ip(peer)
        except SystemExit:
            continue
        sh("docker exec %s iptables -A INPUT -s %s -j DROP" % (victim, peer_ip), check=True)
        sh("docker exec %s iptables -A OUTPUT -d %s -j DROP" % (victim, peer_ip), check=True)
        cut.append((peer, peer_ip))
    print("  cut from: %s" % [p for p, _ in cut])

    print("  isolated for %.1fs..." % args.duration)
    time.sleep(args.duration)

    for _, peer_ip in cut:
        sh("docker exec %s iptables -D INPUT -s %s -j DROP" % (victim, peer_ip))
        sh("docker exec %s iptables -D OUTPUT -d %s -j DROP" % (victim, peer_ip))
    print("  healed. NOTE: %s returns with a much higher term and will depose"
          % victim)
    print("  the sitting leader — isolation is not free even after it ends.")
    gt_resolved(rec["gt_id"], "injector")
    _ = victim_ip


def cmd_latency(args):
    node = args.target
    assert_net_admin(node)
    print("latency: %s +%dms for %.1fs" % (node, args.ms, args.duration))
    rec = gt_injected("latency_spike", node, args.duration)

    r = sh("docker exec %s tc qdisc add dev eth0 root netem delay %dms" % (node, args.ms))
    if r.returncode != 0:
        raise SystemExit("tc failed (iproute2 / NET_ADMIN?): %s" % r.stderr.strip()[:300])

    time.sleep(args.duration)
    sh("docker exec %s tc qdisc del dev eth0 root" % node)
    print("  delay removed.")
    gt_resolved(rec["gt_id"], "injector")


def cmd_leak(args):
    node = args.target
    print("leak: %s at %.1f MB/s (real allocation)" % (node, args.rate_mb))
    gt_injected("memory_leak", node)
    ok, detail = admin_action(node, "START_LEAK", args.rate_mb)
    print("  %s: %s" % ("started" if ok else "FAILED", detail))
    print("  NOT stopping — recovery is the system under test.")


def cmd_cpu(args):
    node = args.target
    print("cpu: %s with %d burn workers (real cycles)" % (node, args.workers))
    gt_injected("cpu_overload", node)
    ok, detail = admin_action(node, "START_CPU_BURN", args.workers)
    print("  %s: %s" % ("started" if ok else "FAILED", detail))
    print("  NOT stopping — recovery is the system under test.")


def cmd_clear(args):
    """Undo self-inflicted faults. For harness cleanup between gate runs —
    NOT a recovery action, and never counted as one."""
    for node in discover_nodes():
        for verb in ("STOP_LEAK", "STOP_CPU_BURN"):
            try:
                ok, detail = admin_action(node, verb)
                if ok:
                    print("  %s %s: %s" % (node, verb, detail))
            except SystemExit:
                pass


def cmd_list(args):
    nodes = discover_nodes()
    print("node containers (label %s):" % NODE_LABEL)
    for n in nodes:
        print("  %-10s %s" % (n, container_ip(n)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("crash"); p.add_argument("--target", required=True); p.set_defaults(fn=cmd_crash)

    p = sub.add_parser("partition")
    p.add_argument("--a", required=True); p.add_argument("--b", required=True)
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--blackout", action="store_true",
                   help="also drop Kafka traffic (plan.md §5.6 hard case)")
    p.set_defaults(fn=cmd_partition)

    p = sub.add_parser("isolate", help="cut a node from EVERY peer (majority cut)")
    p.add_argument("--target", required=True)
    p.add_argument("--duration", type=float, default=30.0)
    p.set_defaults(fn=cmd_isolate)

    p = sub.add_parser("latency")
    p.add_argument("--target", required=True)
    p.add_argument("--ms", type=int, default=300)
    p.add_argument("--duration", type=float, default=10.0)
    p.set_defaults(fn=cmd_latency)

    p = sub.add_parser("leak")
    p.add_argument("--target", required=True)
    p.add_argument("--rate-mb", type=float, default=5.0)
    p.set_defaults(fn=cmd_leak)

    p = sub.add_parser("cpu")
    p.add_argument("--target", required=True)
    p.add_argument("--workers", type=int, default=2)
    p.set_defaults(fn=cmd_cpu)

    sub.add_parser("clear").set_defaults(fn=cmd_clear)
    sub.add_parser("list").set_defaults(fn=cmd_list)

    args = ap.parse_args()
    args.fn(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
