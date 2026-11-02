#!/usr/bin/env python3
"""Phase 8 gate — the full action set (plan.md §7).

Every action in §10's reality matrix claims to be "real". This makes each one
prove it by observing the effect, not by trusting a `completed` result:

  reroute_traffic   weight table changes AND traffic actually moves
  rebalance_load    weights flatten, shed nodes excluded
  load_shed         node refuses clients BUT KEEPS QUORUM  <- the key one
  promote_replica   leadership actually moves (winner not ours to choose)
  restart_node      genuinely clears leaked RSS

Intents are injected directly into cluster.actions rather than waiting for the
classifier to produce them, because this gate tests the EXECUTOR's actions, not
the diagnosis path (Phase 5 covers that).
"""

import json
import subprocess
import sys
import time
import uuid

NODES = 5
passed, failed, notes = [], [], []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label, (" — " + detail) if detail else ""))
    return ok


def note(text):
    notes.append(text)
    print("  NOTE  %s" % text)


def sh(cmd, timeout=200):
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
    return False


def consume(topic, seconds=8, max_msgs=400, from_start=True):
    r = sh("docker compose exec -T kafka timeout %d "
           "/opt/kafka/bin/kafka-console-consumer.sh "
           "--bootstrap-server localhost:9092 --topic %s "
           "--max-messages %d --timeout-ms %d %s 2>/dev/null"
           % (seconds + 12, topic, max_msgs, seconds * 1000,
              "--from-beginning" if from_start else ""),
           timeout=seconds + 50)
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def health(seconds=7):
    return consume("cluster.health", seconds, from_start=False)


def latest(rows):
    out = {}
    for d in rows:
        n = d.get("node_id")
        if n and (n not in out or d["ts"] >= out[n]["ts"]):
            out[n] = d
    return out


def weights():
    script = ("import grpc, loadgen_pb2, loadgen_pb2_grpc, json\n"
              "st=loadgen_pb2_grpc.LoadGenServiceStub("
              "grpc.insecure_channel('localhost:9500'))\n"
              "print(json.dumps(dict(st.GetWeights(loadgen_pb2.Empty()).weights)))\n")
    r = sh("docker compose exec -T load_generator python -c \"%s\""
           % script.replace('"', '\\"'), timeout=60)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {}


def emit(action, target, expected_status="healthy", boot_id="", started_at=0.0):
    """Publish an intent straight into cluster.actions."""
    iid = "%s:gate8:%d" % (target, int(time.time() * 1000))
    intent = {
        "decision_id": uuid.uuid4().hex[:16], "incident_id": iid,
        "attempt_no": 1, "action": action, "target": target,
        "source": "gate8", "confidence": 100, "reason": "phase 8 gate",
        "decided_at": time.time(),
        "expected_target_status": expected_status,
        "expected_boot_id": boot_id, "expected_started_at": started_at,
        "target_generation": 0, "diagnosis": None,
    }
    payload = json.dumps(intent)
    subprocess.run(
        "docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh "
        "--bootstrap-server localhost:9092 --topic cluster.actions",
        shell=True, input=payload + "\n", capture_output=True, text=True, timeout=60)
    return intent


def result_for(did, tries=10):
    for _ in range(tries):
        time.sleep(3)
        for r in consume("cluster.action_results", 5, from_start=True):
            if r.get("decision_id") == did and r.get("state") in (
                    "completed", "failed", "rejected"):
                return r
    return None


def served(rows, node):
    return sum((d.get("req_ok") or 0) for d in rows if d.get("node_id") == node)


def main():
    print("=== Phase 8 gate: the full action set ===\n")
    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans -v", timeout=240)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up full stack...")
    if sh("docker compose up -d --build", timeout=1500).returncode != 0:
        print(sh("docker compose logs --tail 40 executor").stdout[-2000:])
        return 1
    rc = sh("./scripts/wait_for_warmup.sh", timeout=280).returncode
    if not check("0. warm-up barrier passed", rc == 0):
        return 1

    rows = health(7)
    lat = latest(rows)
    leader = next((n for n, d in lat.items() if d.get("raft_leader")), None)
    serving = [n for n, d in lat.items() if d["role"] in ("Worker", "Gateway")]
    check("0b. cluster healthy", leader is not None and len(serving) == 2,
          "leader=%s serving=%s" % (leader, serving))

    # ------------------------------------------------- reroute_traffic
    victim = serving[0]
    print("\n--- reroute_traffic away from %s ---" % victim)
    w0 = weights()
    t0 = served(health(7), victim)
    i = emit("reroute_traffic", victim)
    r = result_for(i["decision_id"])
    check("1. reroute_traffic completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:90])
    w1 = weights()
    check("2. weight table actually changed",
          w1.get(victim, 1) < w0.get(victim, 0),
          "%s: %.3f -> %.3f" % (victim, w0.get(victim, 0), w1.get(victim, 0)))
    time.sleep(12)
    t1 = served(health(7), victim)
    check("3. REAL traffic followed the weights", t1 < t0,
          "%s served %d -> %d" % (victim, t0, t1))

    # ------------------------------------------------- rebalance_load
    print("\n--- rebalance_load ---")
    i = emit("rebalance_load", leader)
    r = result_for(i["decision_id"])
    check("4. rebalance_load completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:90])
    w2 = weights()
    spread = (max(w2.values()) - min(w2.values())) if w2 else 1.0
    check("5. weights flattened", spread < 0.05, "spread=%.4f" % spread)

    # ------------------------------------- load_shed: KEEPS QUORUM
    shed_target = serving[1]
    print("\n--- load_shed %s (must KEEP Raft participation) ---" % shed_target)
    q0 = sum(1 for d in lat.values() if d.get("has_quorum"))
    i = emit("load_shed", shed_target)
    r = result_for(i["decision_id"])
    check("6. load_shed completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:90])
    time.sleep(10)
    rows3 = health(7)
    lat3 = latest(rows3)
    check("7. node reports shedding", lat3.get(shed_target, {}).get("shedding") is True,
          "shedding=%s" % lat3.get(shed_target, {}).get("shedding"))
    check("8. shed node STILL publishes (not isolated)", shed_target in lat3,
          "present=%s" % (shed_target in lat3))
    q1 = sum(1 for d in lat3.values() if d.get("has_quorum"))
    check("9. QUORUM UNAFFECTED by shedding", q1 >= q0 - 1 and q1 > 0,
          "nodes with quorum %d -> %d" % (q0, q1))
    check("10. shed node's weight zeroed", weights().get(shed_target, 1.0) == 0.0,
          "weight=%.3f" % weights().get(shed_target, -1))

    print("\n--- load_shed_readmit %s ---" % shed_target)
    i = emit("load_shed_readmit", shed_target)
    r = result_for(i["decision_id"])
    check("11. readmit completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:90])
    time.sleep(8)
    check("12. node accepting requests again",
          latest(health(7)).get(shed_target, {}).get("shedding") is False)

    # ------------------------------------------------ promote_replica
    print("\n--- promote_replica (forced step-down, winner not ours) ---")
    before = leader
    i = emit("promote_replica", before, expected_status="healthy")
    r = result_for(i["decision_id"], tries=12)
    check("13. promote_replica completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:110])
    time.sleep(8)
    after = next((n for n, d in latest(health(7)).items() if d.get("raft_leader")), None)
    check("14. leadership actually moved", after is not None and after != before,
          "%s -> %s" % (before, after))
    note("the executor records intent vs ACTUAL winner separately — Raft "
         "chooses, we only force the election (§0.2).")

    # --------------------------------- restart genuinely clears a leak
    leak_target = serving[0]
    print("\n--- leak %s, then restart_node (must clear real RSS) ---" % leak_target)
    sh("python3 failure_injector/inject.py leak --target %s --rate-mb 8"
       % leak_target, timeout=120)
    time.sleep(16)
    peak = max((d.get("rss_mb") or 0) for d in health(7)
               if d.get("node_id") == leak_target)
    boot = latest(health(5)).get(leak_target, {}).get("boot_id", "")
    i = emit("restart_node", leak_target, expected_status="healthy", boot_id=boot)
    r = result_for(i["decision_id"], tries=12)
    check("15. restart_node completed", r and r["state"] == "completed",
          (r or {}).get("detail", "no result")[:90])
    time.sleep(12)
    after_rss = max((d.get("rss_mb") or 0) for d in health(7)
                    if d.get("node_id") == leak_target)
    check("16. restart genuinely cleared the leaked RSS",
          peak > 120 and after_rss < peak * 0.5,
          "%.0f MB -> %.0f MB" % (peak, after_rss))
    note("the simulator models this as `health = 90`; here the address space "
         "is actually destroyed, which is strictly more honest.")

    sh("python3 failure_injector/inject.py clear", timeout=120)

    print("\n" + "=" * 64)
    print("  Phase 8 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    for n in notes:
        print("    - %s" % n)
    print("=" * 64)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
