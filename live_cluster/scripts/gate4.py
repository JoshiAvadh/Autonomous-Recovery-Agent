#!/usr/bin/env python3
"""Phase 4 gate — failure injector (plan.md §7).

Beyond "all 5 types degrade health", this settles two open questions that
would otherwise be discovered in Phase 5 when the classifier quietly fails:

  Q1  Is `tc netem` visible in node telemetry at all? Reads are served from
      local state and cannot carry path delay; only writes (quorum round-trip)
      can. If the leader's p99 does not move under injected delay, the §5.2
      latency rule needs a peer-RTT probe instead of req_p99_ms.

  Q2  Does the §5.6 caveat hold in practice — a partitioned node keeps
      publishing to Kafka while its peers mark it unreachable? That asymmetry
      is the entire basis of classifier rule 2.
"""

import json
import subprocess
import sys
import time

NODES = 5
passed, failed, notes = [], [], []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label, (" — " + detail) if detail else ""))
    return ok


def note(text):
    notes.append(text)
    print("  NOTE  %s" % text)


def sh(cmd, timeout=180):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def sh_bg(cmd):
    """Launch without waiting.

    `subprocess.run(cmd + " &", capture_output=True)` does NOT background: the
    captured pipe stays open on the child, so communicate() blocks until it
    exits anyway. Popen with the pipes detached is the only way to actually
    return while the injection is still running — which is required for the
    timed faults (latency, partition), where the gate must observe the cluster
    DURING the fault window rather than after it heals.
    """
    return subprocess.Popen(
        cmd, shell=True, cwd=None,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


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


def consume(topic, seconds=6, max_msgs=None, from_start=False):
    """from_start matters: cluster.health is a live 1 Hz stream so latest is
    right, but cluster.failures is a SPARSE HISTORICAL LOG written minutes
    earlier — reading it from latest returns nothing, which is what made
    checks 7 and 9 fail against a ground-truth stream that was perfectly fine."""
    budget = max_msgs if max_msgs else seconds * NODES + NODES
    r = sh(
        "docker compose exec -T kafka timeout %d "
        "/opt/kafka/bin/kafka-console-consumer.sh "
        "--bootstrap-server localhost:9092 --topic %s "
        "--max-messages %d --timeout-ms %d %s 2>/dev/null"
        % (seconds + 10, topic, budget, seconds * 1000,
           "--from-beginning" if from_start else ""),
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


def health(seconds=6):
    return consume("cluster.health", seconds)


def latest(rows):
    out = {}
    for d in rows:
        nid = d.get("node_id")
        if nid and (nid not in out or d["ts"] >= out[nid]["ts"]):
            out[nid] = d
    return out


def leader_of(rows):
    ls = [d for d in rows if d.get("raft_leader")]
    return max(ls, key=lambda d: d["ts"])["node_id"] if ls else None


def peak(rows, node, field):
    vals = [d[field] for d in rows if d.get("node_id") == node and d.get(field) is not None]
    return max(vals) if vals else None


def inject(args, timeout=180):
    return sh("python3 failure_injector/inject.py %s" % args, timeout=timeout)


def main():
    print("=== Phase 4 gate: failure injector ===\n")
    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans", timeout=200)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up cluster...")
    if sh("docker compose up -d --build", timeout=900).returncode != 0:
        return 1
    print("waiting for election + traffic...")
    time.sleep(40)

    rows = health(6)
    leader = leader_of(rows)
    check("0. cluster healthy, leader elected", leader is not None, str(leader))
    if not leader:
        return 1

    followers = [n for n in ("node-1", "node-2", "node-3") if n != leader]
    serving = ["node-4", "node-5"]

    # ---------------------------------------------------------------- Q1: tc
    print("\n--- latency: +400ms on the LEADER (%s) ---" % leader)
    base_p99 = peak(rows, leader, "req_p99_ms")
    print("      leader p99 before: %s ms" % base_p99)

    lat_proc = sh_bg("python3 failure_injector/inject.py latency --target %s "
                     "--ms 400 --duration 25" % leader)
    time.sleep(10)
    during = health(8)
    lat_p99 = peak(during, leader, "req_p99_ms")
    print("      leader p99 during: %s ms" % lat_p99)

    tc_visible = (lat_p99 is not None and base_p99 is not None and lat_p99 > base_p99 * 2)
    check("1. tc netem is visible in write latency (Q1)", tc_visible,
          "%s -> %s ms" % (base_p99, lat_p99))
    if not tc_visible:
        note("Q1 ANSWERED NO: req_p99_ms does not carry path delay. "
             "§5.2 rule 5 needs a peer-RTT probe, not req_p99_ms.")
    else:
        note("Q1 ANSWERED YES: write-path latency carries netem; "
             "§5.2 rule 5 can use req_p99_ms, baselined per-node.")

    # follower reads should be largely unaffected — the contrast is the point
    f_p99 = peak(during, serving[0], "req_p99_ms")
    print("      serving-tier p99 during: %s ms (reads are local)" % f_p99)
    lat_proc.wait(timeout=60)   # let the injector heal the qdisc

    # -------------------------------------------------------- Q2: partition
    # A MAJORITY cut, not a single link. §5.5a measured that a single-link cut
    # in a 3-voter cluster is absorbed by Raft and leaves no trace — asserting
    # on it would penalise the classifier for being correct.
    victim = followers[0]
    print("\n--- isolate %s from ALL peers (majority cut) ---" % victim)
    base_term = max(d.get("raft_term", 0) for d in rows)
    part_proc = sh_bg("python3 failure_injector/inject.py isolate --target %s "
                      "--duration 40" % victim)
    time.sleep(14)
    prows = health(8)
    plat = latest(prows)

    v_term = plat.get(victim, {}).get("raft_term", 0)
    maj_terms = [d.get("raft_term", 0) for n, d in plat.items()
                 if n != victim and d.get("role") in ("Primary", "Replica")]
    maj_term = max(maj_terms) if maj_terms else 0
    check("2. isolated node's term diverges upward", v_term >= maj_term + 3,
          "victim=%s majority=%s (base=%s)" % (v_term, maj_term, base_term))

    lead2 = leader_of(prows)
    ml = (plat.get(lead2, {}).get("match_lag") or {}) if lead2 else {}
    check("3. leader's match_lag for isolated node grows", (ml.get(victim) or 0) > 5,
          "match_lag=%s" % ml)

    check("4. isolated node loses quorum", plat.get(victim, {}).get("has_quorum") is False,
          "has_quorum=%r" % plat.get(victim, {}).get("has_quorum"))

    still_publishing = victim in plat
    check("5. isolated node KEEPS publishing telemetry (§5.6)", still_publishing,
          "present=%s" % still_publishing)

    pr = plat.get(victim, {}).get("peers_reachable") or {}
    note("peers_reachable during isolation = %s. It DOES flip here (the "
         "campaigning node churns connections), but stayed True for 45s under "
         "a single-link cut — conditional on connection churn, so rule 2 keys "
         "on term + match_lag instead (§5.5a)." % pr)
    part_proc.wait(timeout=90)

    # ------------------------------------------------------------ leak / cpu
    print("\n--- memory leak on %s (real allocation) ---" % serving[0])
    r0 = peak(health(5), serving[0], "rss_mb")
    inject("leak --target %s --rate-mb 8" % serving[0])
    time.sleep(18)
    r1 = peak(health(6), serving[0], "rss_mb")
    check("6. rss_mb genuinely climbs under leak", r0 and r1 and r1 > r0 + 30,
          "%.1f -> %.1f MB" % (r0 or 0, r1 or 0))

    # THE assertion no gate had. `health` was hardcoded to 100.0 for eight
    # phases and nothing noticed, because every gate watched rss_mb or
    # cpu_pct — the raw inputs — and never the SCORE derived from them. A node
    # 385 MB over baseline reported health=100, status=healthy, which silently
    # disabled every health-keyed branch in the reused decision code.
    # Scan the WHOLE topic, not the current window. This gate was written in
    # Phase 4, when nothing recovered anything; it now runs in a Phase 8 world
    # where the agent diagnoses the leak and the executor restarts the node.
    # Sampling "now" races that recovery: the peak RSS is observed before it
    # and the health reading lands after, by which time the node has a fresh
    # boot and a reset baseline. Measured directly — health fell
    # 100 -> 87.9 -> 75.8 -> 63.8 (degraded) and then reset to 100 on restart.
    hist_rows = consume("cluster.health", 10, max_msgs=1500, from_start=True)
    hs = [d["health"] for d in hist_rows if d.get("node_id") == serving[0]
          and d.get("health") is not None]
    sts = {d["status"] for d in hist_rows if d.get("node_id") == serving[0]}
    check("6b. HEALTH SCORE fell under the leak at some point",
          bool(hs) and min(hs) < 70.0, "health min=%s" % (min(hs) if hs else None))
    check("6c. status degraded at some point",
          "degraded" in sts or "failed" in sts, "statuses seen: %s" % sorted(sts))

    print("\n--- cpu burn on %s (real cycles) ---" % serving[1])
    c0 = peak(health(5), serving[1], "cpu_pct")
    inject("cpu --target %s --workers 2" % serving[1])
    time.sleep(15)
    c1 = peak(health(6), serving[1], "cpu_pct")
    check("7. cpu_pct genuinely rises under burn", c0 is not None and c1 and c1 > max(20.0, c0 * 3),
          "%.1f -> %.1f %%" % (c0 or 0, c1 or 0))

    inject("clear")

    # ----------------------------------------------------------------- crash
    victim = serving[1]
    print("\n--- crash: %s ---" % victim)
    boots_before = {d.get("boot_id") for d in health(6)
                    if d.get("node_id") == victim and d.get("boot_id")}
    inject("crash --target %s" % victim)
    time.sleep(12)
    # Same correction: `docker kill` no longer means "stays dead". The
    # executor restarts crashed nodes, so the Phase 4 assertion that the node
    # is ABSENT now fails for the best possible reason — it was recovered.
    # A crash is proven by the node going away and coming back under a NEW
    # boot_id, or by it still being gone. Either is a real crash; only "never
    # went away at all" is a failure.
    # Sample boot_id BEFORE and AFTER rather than scanning history. The first
    # attempt used from_start=True with max_msgs=400, which reads the OLDEST
    # 400 messages — by crash time the topic holds thousands, so the scan only
    # ever saw the run's opening minute and reported one boot_id no matter
    # what happened. Same mistake as reading cluster.failures from `latest`:
    # the wrong window, not the wrong data.
    boots_after = {d.get("boot_id") for d in health(10)
                   if d.get("node_id") == victim and d.get("boot_id")}
    distinct_boots = len(boots_before | boots_after)
    after = latest(health(8))
    gone = victim not in after
    recovered = distinct_boots > 1
    check("8. crash was real (node died; stayed dead OR was recovered)",
          gone or recovered,
          "absent=%s distinct_boot_ids=%d" % (gone, distinct_boots))
    if recovered and not gone:
        note("the crashed node was RESTARTED by the executor mid-gate — this "
             "Phase 4 gate now runs in a Phase 8 world that repairs its faults.")

    # --------------------------------------------------------- ground truth
    print("\n--- ground truth ---")
    gt = consume("cluster.failures", 6, max_msgs=40, from_start=True)
    kinds = [g.get("failure_type") for g in gt if g.get("event") == "injected"]
    resolved = [g for g in gt if g.get("event") == "resolved"]
    check("9. all 5 failure types recorded as ground truth",
          set(kinds) >= {"latency_spike", "network_partition", "memory_leak",
                         "cpu_overload", "crash"},
          str(sorted(set(kinds))))
    check("10. every injected record has a gt_id",
          all(g.get("gt_id") for g in gt), "%d records" % len(gt))
    check("11. only self-healing faults resolved by injector",
          all(g.get("resolved_by") == "injector" for g in resolved) and len(resolved) >= 2,
          "%d resolved: %s" % (len(resolved), [g.get("resolved_by") for g in resolved]))
    note("crash/leak/cpu correctly have NO resolved record — "
         "the recovery system undoes those, and it does not exist until Phase 6.")

    print("\n" + "=" * 62)
    print("  Phase 4 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    print("\n  Findings:")
    for n in notes:
        print("    - %s" % n)
    print("=" * 62)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
