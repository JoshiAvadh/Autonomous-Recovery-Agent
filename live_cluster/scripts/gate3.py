#!/usr/bin/env python3
"""Phase 3 gate — load generator (plan.md §7).

The gate is deliberately blunt: shift the weights, watch REAL load move. If it
does not, every load-based action in Phase 8 (reroute_traffic, rebalance_load,
load_shed) is fiction, and the right time to find that out is now.

Also asserts the §5.5 abstention contract: p99 must be None below the sample
floor, not 0.0. Returning 0.0 there would read as "fast" and silently defeat
the latency rule's refusal to fire on thin data.
"""

import json
import subprocess
import sys
import time

NODES = 5
LOADGEN_PORT = 9500

passed, failed = [], []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label, (" — " + detail) if detail else ""))
    return ok


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def wait_for_docker(timeout=150):
    if sh("docker info", timeout=20).returncode == 0:
        return True
    sys.stdout.write("waiting for docker daemon ")
    sys.stdout.flush()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if sh("docker info", timeout=20).returncode == 0:
            print(" ready")
            return True
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(3)
    print("\ndocker daemon not reachable — start Docker Desktop and re-run.")
    return False


def sample_health(seconds=6):
    budget = seconds * NODES + NODES
    r = sh(
        "docker compose exec -T kafka timeout %d "
        "/opt/kafka/bin/kafka-console-consumer.sh "
        "--bootstrap-server localhost:9092 --topic cluster.health "
        "--max-messages %d --timeout-ms %d 2>/dev/null"
        % (seconds + 10, budget, seconds * 1000),
        timeout=seconds + 45,
    )
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def per_node_traffic(rows):
    """Total served requests per node across the sample (windowed counters)."""
    agg = {}
    for d in rows:
        nid = d.get("node_id")
        if not nid:
            continue
        served = (d.get("req_ok") or 0) + (d.get("req_fail") or 0) + (d.get("req_timeout") or 0)
        agg[nid] = agg.get(nid, 0) + served
    return agg


def loadgen(method, body="{}"):
    """Call the load generator's control plane from inside its own container."""
    script = (
        "import grpc, json, loadgen_pb2, loadgen_pb2_grpc\n"
        "from google.protobuf import json_format\n"
        "st = loadgen_pb2_grpc.LoadGenServiceStub(grpc.insecure_channel('localhost:%d'))\n"
        "req = json_format.Parse(%r, loadgen_pb2.%s())\n"
        "r = st.%s(req)\n"
        "print(json_format.MessageToJson(r, indent=0).replace(chr(10),''))\n"
        % (LOADGEN_PORT, body,
           {"SetWeights": "WeightTable", "GetWeights": "Empty",
            "SetRate": "RateRequest", "GetStats": "Empty"}[method],
           method)
    )
    r = sh("docker compose exec -T load_generator python -c \"%s\"" % script.replace('"', '\\"'),
           timeout=60)
    txt = (r.stdout or "").strip()
    try:
        return json.loads(txt.splitlines()[-1]) if txt else {}
    except (ValueError, IndexError):
        return {"_raw": txt, "_err": (r.stderr or "")[:300]}


def main():
    print("=== Phase 3 gate: load generator ===\n")
    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans", timeout=180)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up %d nodes + load generator..." % NODES)
    up = sh("docker compose up -d --build", timeout=900)
    if up.returncode != 0:
        print(up.stderr[-2500:])
        return 1

    print("waiting for election + traffic to establish...")
    time.sleep(40)

    # --- traffic is actually flowing -----------------------------------------
    stats = loadgen("GetStats")
    sent = int(stats.get("sent", 0))
    check("1. load generator is sending", sent > 0, "sent=%s leader=%s" % (sent, stats.get("leader")))
    # `ok` counts reads AND writes, so it was never evidence about writes.
    # commit_index advancing is — only a committed Raft write moves it.
    ci0 = max([d.get("commit_index", 0) for d in sample_health(5)] or [0])
    time.sleep(8)
    ci1 = max([d.get("commit_index", 0) for d in sample_health(5)] or [0])
    check("2. writes are committing (commit_index advances)", ci1 > ci0,
          "commit_index %d -> %d; loadgen ok=%s failed=%s"
          % (ci0, ci1, stats.get("ok"), stats.get("failed")))

    rows = sample_health(8)
    traffic0 = per_node_traffic(rows)
    print("      traffic per node (uniform weights): %s" % traffic0)
    serving = ["node-4", "node-5"]
    check("3. all nodes receiving reads",
          all(traffic0.get(n, 0) > 0 for n in serving),
          str({n: traffic0.get(n, 0) for n in serving}))

    # --- §5.5 abstention: p99 is None below the floor, never 0.0 -------------
    idle = [d for d in rows if (d.get("req_ok") or 0) + (d.get("req_fail") or 0) == 0]
    check("4. p99 is None (not 0.0) when no requests in window",
          all(d.get("req_p99_ms") is None for d in idle) if idle else True,
          "%d idle windows checked" % len(idle))

    # --- THE gate: move the weights, watch real load follow -------------------
    print("\n--- zeroing node-4, sending its share to node-5 ---")
    res = loadgen("SetWeights", '{"weights": {"node-4": 0.0, "node-5": 1.0}}')
    check("5. SetWeights accepted", res.get("ok") is True, str(res)[:160])

    time.sleep(12)
    rows2 = sample_health(8)
    traffic1 = per_node_traffic(rows2)
    print("      traffic per node (node-4 zeroed):  %s" % traffic1)

    n4_before, n4_after = traffic0.get("node-4", 0), traffic1.get("node-4", 0)
    n5_before, n5_after = traffic0.get("node-5", 0), traffic1.get("node-5", 0)

    check("6. node-4 traffic collapsed", n4_after < max(1, n4_before * 0.25),
          "%d -> %d" % (n4_before, n4_after))
    check("7. node-5 traffic rose", n5_after > n5_before,
          "%d -> %d" % (n5_before, n5_after))

    # --- restore, and confirm it is reversible -------------------------------
    print("\n--- restoring uniform weights ---")
    uniform = json.dumps({"weights": {("node-%d" % i): 1.0 for i in range(1, NODES + 1)}})
    loadgen("SetWeights", uniform)
    time.sleep(12)
    traffic2 = per_node_traffic(sample_health(8))
    print("      traffic per node (restored):       %s" % traffic2)
    check("8. node-4 traffic recovered", traffic2.get("node-4", 0) > n4_after,
          "%d -> %d" % (n4_after, traffic2.get("node-4", 0)))

    print("\n" + "=" * 58)
    print("  Phase 3 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    print("=" * 58)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
