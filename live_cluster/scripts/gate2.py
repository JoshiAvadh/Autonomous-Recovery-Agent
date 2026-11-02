#!/usr/bin/env python3
"""Phase 2 gate — real Raft (plan.md §7).

Five assertions, because a term increment ALONE is not evidence of a successful
election: Raft bumps the term on candidacy, not on victory, so a node that
repeatedly stands and loses increments terms forever without any leader being
established. Assertions 2 (stabilizes) and 4 (commit advances) are what
separate a real failover from election flapping.

Phase 3's load generator does not exist yet, so this drives its own writes —
commit_index cannot advance without traffic.

Host-side, Python 3.9 compatible, no third-party deps.
"""

import json
import subprocess
import sys
import time

NODES = 5
VOTERS = 3

passed = []
failed = []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label, (" — " + detail) if detail else ""))
    return ok


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def wait_for_docker(timeout=150):
    """Docker Desktop takes ~30s to expose the socket after launch. Without
    this the gate dies on 'unable to get image' and reads like a broken build
    rather than a machine that simply is not ready. Same reasoning as the
    Phase 6 warm-up barrier: make the wait visible and intentional."""
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
    """Collect cluster.health for ~N seconds -> list of dicts (chronological).

    `--timeout-ms` alone is NOT enough: it means "exit after N ms with no
    message", and at 1 Hz x NODES the stream never idles, so the consumer would
    run forever. `--max-messages` bounds the flowing case, `--timeout-ms` the
    idle one, and `timeout` is the backstop if both fail.
    """
    budget = seconds * NODES + NODES          # ~1 msg/node/sec, plus slack
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


def latest_per_node(rows):
    latest = {}
    for d in rows:
        nid = d.get("node_id")
        if nid and (nid not in latest or d["ts"] >= latest[nid]["ts"]):
            latest[nid] = d
    return latest


def find_leader(rows):
    leaders = [d for d in rows if d.get("raft_leader")]
    if not leaders:
        return None, None
    newest = max(leaders, key=lambda d: d["ts"])
    return newest["node_id"], newest.get("raft_term", 0)


def wait_for_leader(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = sample_health(5)
        nid, term = find_leader(rows)
        if nid:
            return nid, term, rows
    return None, None, []


def write_via(node, key, value):
    """SubmitWrite through a node's own gRPC endpoint, from inside its container."""
    script = (
        "import grpc, node_pb2, node_pb2_grpc\n"
        "st = node_pb2_grpc.NodeServiceStub(grpc.insecure_channel('localhost:8000'))\n"
        "r = st.SubmitWrite(node_pb2.WriteRequest(key=%r, value=%r))\n"
        "print('%%s|%%s|%%s' %% (r.committed, r.commit_index, r.error))\n" % (key, value)
    )
    r = sh("docker compose exec -T %s python -c \"%s\"" % (node, script.replace('"', '\\"')), timeout=60)
    line = (r.stdout or "").strip().splitlines()
    if not line:
        return False, 0, "no_output:" + (r.stderr or "")[:120]
    parts = line[-1].split("|")
    return parts[0] == "True", int(parts[1] or 0), parts[2]


def main():
    print("=== Phase 2 gate: real Raft ===\n")

    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans", timeout=180)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up %d nodes..." % NODES)
    up = sh("docker compose up -d --build", timeout=600)
    if up.returncode != 0:
        print(up.stderr[-2000:])
        return 1

    print("waiting for first election...")
    time.sleep(25)
    leader, term0, rows = wait_for_leader()
    if not check("1. leader elected", leader is not None, str(leader)):
        return 1
    print("      leader=%s term=%s" % (leader, term0))

    # --- tier split: voters vs the serving tier -----------------------------
    latest = latest_per_node(rows)
    check("0. all %d nodes reporting" % NODES, len(latest) == NODES, "got %d" % len(latest))

    voters = [d for d in latest.values() if d["role"] in ("Primary", "Replica")]
    serving = [d for d in latest.values() if d["role"] in ("Worker", "Gateway")]
    check("0b. %d voters / %d serving" % (VOTERS, NODES - VOTERS),
          len(voters) == VOTERS and len(serving) == NODES - VOTERS,
          "%d/%d" % (len(voters), len(serving)))

    # peers_reachable must be ADDRESS-keyed and never contain counter ids (§4.6a)
    pr = latest[leader].get("peers_reachable") or {}
    check("0c. peers_reachable address-keyed, no counter ids",
          len(pr) > 0 and all(not k.isdigit() for k in pr),
          str(pr))

    # match_lag: leader-only, voters-only
    ml = latest[leader].get("match_lag")
    non_leader = [d for d in latest.values() if not d.get("raft_leader")]
    check("0d. match_lag on leader only",
          ml is not None and all(d.get("match_lag") is None for d in non_leader),
          "leader=%s" % ml)

    # --- writes commit before the kill --------------------------------------
    ok, ci_before, err = write_via(leader, "pre", "1")
    check("0e. write commits on leader", ok, err or ("commit_idx=%d" % ci_before))

    print("\n--- killing the leader (%s) ---" % leader)
    sh("docker kill %s" % leader, timeout=60)
    time.sleep(12)

    new_leader, term1, rows2 = wait_for_leader()
    latest2 = latest_per_node(rows2)

    # 1. leadership actually moved
    check("2. a DIFFERENT node is leader",
          new_leader is not None and new_leader != leader,
          "%s -> %s" % (leader, new_leader))
    if not new_leader:
        return 1

    # 2. term increased...
    check("3. raft_term increased", (term1 or 0) > (term0 or 0),
          "%s -> %s" % (term0, term1))

    # ...and then STABILIZED (a flapping candidate bumps terms forever)
    print("      watching 8s for term stability...")
    stab = sample_health(8)
    terms = sorted({d.get("raft_term", 0) for d in stab if d.get("node_id") != leader})
    check("4. raft_term stabilized (no flapping)", len(terms) <= 1,
          "terms seen: %s" % terms)

    # 3. quorum intact on the survivor
    nl = latest2.get(new_leader, {})
    check("5. new leader has quorum", nl.get("raft_leader") is True, "role=%s" % nl.get("role"))

    # 4. THE liveness proof: commit_index advances AFTER the new leader is up
    ok2, ci_after, err2 = write_via(new_leader, "post", "2")
    check("6. write commits on NEW leader", ok2, err2)
    check("7. commit_index advanced after failover", ci_after > ci_before,
          "%d -> %d" % (ci_before, ci_after))

    # 5. the serving tier survived the failover
    serving2 = [d for d in latest2.values() if d["role"] in ("Worker", "Gateway")]
    lags = [d.get("apply_lag", 0) for d in serving2]
    check("8. serving tier still replicating (apply_lag bounded)",
          len(serving2) == NODES - VOTERS and all(l <= 2 for l in lags),
          "apply_lag=%s" % lags)

    # role must follow reality: the old leader is gone, exactly one Primary
    primaries = [d["node_id"] for d in latest2.values() if d["role"] == "Primary"]
    check("9. exactly one Primary, derived from real leadership",
          len(primaries) == 1, str(primaries))

    print("\n" + "=" * 58)
    print("  Phase 2 gate: %d passed, %d failed" % (len(passed), len(failed)))
    if failed:
        for f in failed:
            print("    FAILED: %s" % f)
    print("=" * 58)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
