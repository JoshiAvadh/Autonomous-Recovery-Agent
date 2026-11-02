#!/usr/bin/env python3
"""Phase 6 gate — policy + executor. PRINCIPAL DEMO CHECKPOINT (plan.md §7).

Asserts the whole claim with NO LLM in it:

    real telemetry -> diagnosis without labels -> rule decision ->
    async execution -> independent evaluation

A crash is injected and the cluster recovers UNATTENDED. If this passes, the
project has demonstrated its thesis; everything after is an upgrade to the
policy layer rather than the thing the demo rests on.
"""

import json
import os
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


def logs(service, grep=None):
    out = sh("docker compose logs --no-color %s" % service, timeout=120).stdout
    lines = out.splitlines()
    return [l for l in lines if grep in l] if grep else lines


def main():
    print("=== Phase 6 gate: policy + executor (PRINCIPAL CHECKPOINT) ===\n")
    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans -v", timeout=240)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up cluster + agent + executor...")
    if sh("docker compose up -d --build", timeout=1500).returncode != 0:
        print(sh("docker compose logs --tail 40 executor").stdout[-2000:])
        return 1

    print("\n--- warm-up barrier (MANDATORY before any injection) ---")
    rc = sh("./scripts/wait_for_warmup.sh", timeout=280).returncode
    if not check("1. warm-up barrier passed", rc == 0):
        print(sh("docker compose logs --tail 30 recovery_agent").stdout[-1500:])
        return 1

    # the reused brain must be the REAL one, not a local reimplementation
    agent_log = "\n".join(logs("recovery_agent"))
    # Assert PROVENANCE, not the absence of a traceback. A local
    # reimplementation would also start cleanly and log PHASE 6 — and would
    # void the thesis while every check stayed green.
    where = sh('docker compose exec -T recovery_agent python -c '
               '"import rule_based, scorer, llm_agent;'
               'print(rule_based.__file__, scorer.__file__, llm_agent.__file__)"',
               timeout=60).stdout.strip()
    check("2. the brain is the SIMULATOR's files, not a local copy",
          where.count("/simulator/") == 3 and "Traceback" not in agent_log,
          where[:110] or "import failed")
    check("3. executor announced its host-control scope",
          any("root-equivalent" in l for l in logs("executor")),
          "§11 disclosure present in the log")

    # ---------------------------------------------- THE unattended recovery
    victim = "node-4"
    print("\n--- crash %s, then DO NOTHING ---" % victim)
    before = sh("docker inspect -f '{{.State.StartedAt}}' %s" % victim).stdout.strip()
    sh("python3 failure_injector/inject.py crash --target %s" % victim, timeout=120)

    print("      waiting up to 90s for unattended recovery...")
    recovered_at = None
    deadline = time.time() + 90
    while time.time() < deadline:
        time.sleep(5)
        state = sh("docker inspect -f '{{.State.Running}}' %s" % victim).stdout.strip()
        after = sh("docker inspect -f '{{.State.StartedAt}}' %s" % victim).stdout.strip()
        if state == "true" and after != before:
            recovered_at = time.time()
            break

    check("4. crashed node came back WITHOUT human intervention",
          recovered_at is not None,
          "StartedAt %s -> %s" % (before[-15:], after[-15:]))

    # ------------------------------------------------ the auditable timeline
    print("\n--- the timeline ---")
    diag = [l for l in logs("recovery_agent", "DIAGNOSIS ") if victim in l]
    check("5. diagnosis emitted for the crash, no ground truth read",
          any("crash" in l for l in diag), "%d diagnosis lines" % len(diag))
    for l in diag[-2:]:
        print("      %s" % l.split("[agent]")[-1].strip()[:140])

    intents = [i for i in consume("cluster.actions") if i.get("target") == victim]
    check("6. intent published to cluster.actions", bool(intents),
          "%d intents" % len(intents))
    if intents:
        i = intents[-1]
        print("      intent: %s %s src=%s conf=%s expected_boot=%s status=%s"
              % (i["decision_id"], i["action"], i["source"], i["confidence"],
                 (i.get("expected_boot_id") or "?")[:8],
                 i.get("expected_target_status")))
        check("7. intent carries incident identity (§6.1)",
              bool(i.get("incident_id")) and i.get("attempt_no", 0) >= 1,
              "incident_id=%s attempt=%s" % (i.get("incident_id"), i.get("attempt_no")))
        check("8. intent carries boot_id expectation, not a duration (§4.1a)",
              "expected_boot_id" in i and "expected_target_uptime" not in i)
        check("9. source is 'rule' — no LLM in this checkpoint",
              i.get("source") == "rule", i.get("source"))

    results = [r for r in consume("cluster.action_results")]
    by_state = {}
    for r in results:
        by_state.setdefault(r.get("state"), []).append(r)
    print("      action_results states: %s"
          % {k: len(v) for k, v in sorted(by_state.items())})

    # Assert all THREE, not just the endpoints. The first version of this check
    # was named for three states but only tested two, and passed on
    # ['accepted', 'completed'] while `applying` was never published at all —
    # a label overstating what it verified.
    check("10. accepted -> applying -> completed all published",
          all(s in by_state for s in ("accepted", "applying", "completed")),
          str(sorted(by_state)))

    if intents:
        did = intents[-1]["decision_id"]
        mine = [r for r in results if r.get("decision_id") == did]
        states = [r.get("state") for r in mine]
        check("11. this decision reached a terminal state",
              "completed" in states or "failed" in states, str(states))
        check("12. 'accepted' is never the last word (§6.3)",
              states[-1] != "accepted" if states else False, str(states))

    # transition log durability
    # Count RECORDS and check their STATES, not lines. `wc -l > 0` is the same
    # weak assertion that let gate7 certify an empty decision_history.json.
    probe = ("import json;"
             "rs=[json.loads(l) for l in open('/state/transitions.jsonl') if l.strip()];"
             "print(len(rs));"
             "print(','.join(sorted({r['state'] for r in rs})))")
    out = sh('docker compose exec -T executor python -c "%s"' % probe,
             timeout=60).stdout.strip().splitlines()
    n = int(out[0]) if out and out[0].isdigit() else 0
    states = out[1] if len(out) > 1 else ""
    check("13. transition log holds real records with real states",
          n > 0 and "applying" in states and "completed" in states,
          "%d records, states=[%s]" % (n, states))

    dup = [r for r in results if r.get("rejected_reason") == "duplicate"]
    note("duplicate rejections: %d (Kafka is at-least-once, so >0 is healthy "
         "idempotency, not an error)" % len(dup))

    print("\n" + "=" * 64)
    print("  Phase 6 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    for n in notes:
        print("    - %s" % n)
    if not failed:
        print("\n  ★ FREEZE POINT: real telemetry -> diagnosis without labels ->")
        print("    rule decision -> async execution -> independent evaluation.")
        print("    No LLM. Tag this before starting Phase 7.")
    print("=" * 64)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
