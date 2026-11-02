#!/usr/bin/env python3
"""Phase 7 gate — LLM tier 2/3, scoring, feedback, cross-run memory.

The thing to guard against here is a gate that passes because nothing crashed.
A missing key, a wrong model, or a reasoning model eating the token budget all
produce the SAME visible outcome as a working system that chose monitor_only:
silence. So this asserts positively that the model actually decided —
`source="llm"` on the wire, a reason string the rules could not have produced,
and a scored outcome persisted to disk.

Tier 2/3 is reached by injecting a memory_leak, whose base confidence is 45
(rule_based.py:63-72) — comfortably under the 75 that would route it to tier 1.
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
                out.append(json.loads(line, strict=False))
            except ValueError:
                pass
    return out


def agent_log(grep=None):
    out = sh("docker compose logs --no-color recovery_agent", timeout=120).stdout
    lines = out.splitlines()
    return [l for l in lines if grep in l] if grep else lines


def main():
    print("=== Phase 7 gate: LLM tier 2/3 + scoring + feedback + memory ===\n")
    if not wait_for_docker():
        return 1

    if not os.path.exists(".env") or "GROQ_API_KEY" not in open(".env").read():
        print("  live_cluster/.env has no GROQ_API_KEY — the LLM path cannot run.")
        return 1

    sh("docker compose down --remove-orphans -v", timeout=240)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up full stack...")
    if sh("docker compose up -d --build", timeout=1500).returncode != 0:
        print(sh("docker compose logs --tail 40 recovery_agent").stdout[-2000:])
        return 1
    if not check("0. warm-up barrier passed",
                 sh("./scripts/wait_for_warmup.sh", timeout=280).returncode == 0):
        return 1

    boot = "\n".join(agent_log())
    check("1. agent announced the Phase 7 pipeline", "PHASE 7" in boot)
    check("2. agent imported the simulator's brain cleanly",
          "Traceback" not in boot, "no import failure")

    # ---- memory_leak: base confidence 45 -> tier 2/3, i.e. the LLM path ----
    victim = "node-4"
    print("\n--- memory_leak on %s (base confidence 45 -> tier 2/3) ---" % victim)
    sh("python3 failure_injector/inject.py leak --target %s --rate-mb 8" % victim,
       timeout=120)

    print("      waiting up to 100s for diagnosis -> routing -> LLM...")
    routed = decided = None
    deadline = time.time() + 100
    while time.time() < deadline:
        time.sleep(6)
        r = agent_log("ROUTED ")
        d = agent_log("LLM DECIDED ")
        routed = routed or (r[-1] if r else None)
        decided = decided or (d[-1] if d else None)
        if decided:
            break

    check("3. diagnosis reached the router",
          bool(agent_log("DIAGNOSIS ")), "%d diagnoses" % len(agent_log("DIAGNOSIS ")))
    check("4. routed to TIER-2/3 (the LLM path)",
          routed is not None and ("TIER-2" in routed or "TIER-3" in routed),
          (routed or "no ROUTED line").split("[agent]")[-1].strip()[:110])

    # The positive assertion: the model actually answered.
    warn = agent_log("LLM returned")
    if warn:
        note("agent logged: %s" % warn[-1].split("[agent]")[-1].strip()[:120])
    check("5. the MODEL decided (not a silent no-op)", decided is not None,
          (decided or "no LLM DECIDED line — key/model/token-budget problem"
           ).split("[agent]")[-1].strip()[:120])

    intents = [i for i in consume("cluster.actions") if i.get("source") == "llm"]
    check("6. an intent carries source='llm' on the wire", bool(intents),
          "%d llm intents" % len(intents))
    if intents:
        i = intents[-1]
        print("      %s %s on %s — %s"
              % (i["decision_id"], i["action"], i["target"], (i.get("reason") or "")[:70]))
        check("7. intent records what the rule WOULD have done",
              "rule_suggestion" in i,
              "rule_suggestion=%s vs llm=%s" % (i.get("rule_suggestion"), i["action"]))
        if i.get("rule_suggestion") and i["rule_suggestion"] != i["action"]:
            note("the model DIVERGED from the rule engine (%s -> %s) — the "
                 "comparison the whole thesis is built on"
                 % (i["rule_suggestion"], i["action"]))

    # ------------------------------------------- scoring / feedback / memory
    print("\n--- outcome loop ---")
    time.sleep(25)
    # A log line is not the deliverable. §4.2 lists cluster.scores as a topic
    # the EVALUATOR consumes, and it was empty: TOPIC_SCORES was imported and
    # never published to, while this check passed off a log string.
    scores = agent_log("SCORE ")
    check("8. decisions are scored ~5s after execution", bool(scores),
          (scores[-1].split("[agent]")[-1].strip()[:110]) if scores else "no SCORE line")

    pub = consume("cluster.scores", 8, max_msgs=50)
    check("8b. scores PUBLISHED to cluster.scores (not just logged)", bool(pub),
          "%d score records on the topic" % len(pub))
    if pub:
        r = pub[-1]
        check("8c. score record carries its 4 components and decision_id",
              all(k in r for k in ("total_score", "speed_score", "stability_score",
                                   "cascade_score", "delivery_score", "decision_id")),
              "total=%s speed=%s stability=%s"
              % (r.get("total_score"), r.get("speed_score"), r.get("stability_score")))

    fb = agent_log("FEEDBACK ")
    check("9. per-action feedback window fired", bool(fb),
          (fb[-1].split("[agent]")[-1].strip()[:110]) if fb else "no FEEDBACK line")
    if fb:
        note("restart_node is judged at 15s, not the simulator's uniform 5 — "
             "a real container is still booting at 5s (§Phase 7).")

    # Count ENTRIES, not lines. The first version used `wc -l`, which a
    # seeded "[]" satisfies — so the check passed while the agent was logging
    # "[MEMORY] Warning: could not save ... Read-only file system" every cycle.
    # A line count is not evidence that anything was learned.
    probe = ("import json;"
             "d=json.load(open('/simulator/decision_history.json'));"
             "print(len(d));"
             "print(json.dumps(d[-1]) if d else '')")
    out = sh('docker compose exec -T recovery_agent python -c "%s"' % probe,
             timeout=60).stdout.strip().splitlines()
    n = int(out[0]) if out and out[0].isdigit() else 0
    check("10. cross-run memory persisted REAL entries", n > 0,
          "%d entries%s" % (n, (" | " + out[1][:90]) if len(out) > 1 else ""))
    warned = agent_log("could not save decision history")
    check("11. no read-only-filesystem warnings from decision_memory",
          not warned,
          warned[-1].split("[agent]")[-1].strip()[:90] if warned else "clean")
    note("memory is bind-mounted from the repo root, so the simulator and the "
         "live system share one learning history (§Phase 7).")

    pred = agent_log("PREDICT ")
    if pred:
        note("predictor fired %d time(s): %s"
             % (len(pred), pred[-1].split("[agent]")[-1].strip()[:90]))

    sh("python3 failure_injector/inject.py clear", timeout=120)

    print("\n" + "=" * 64)
    print("  Phase 7 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    for n in notes:
        print("    - %s" % n)
    print("=" * 64)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
