#!/usr/bin/env python3
"""Phase 5 gate — diagnosis layer (plan.md §7).

Produces the project's FIRST REAL NUMBERS. The agent has run read-only the
whole time: it consumed cluster.health, diagnosed, and logged. It never took
an action and never read a label.

Order matters. The no-injection control runs FIRST: if the warm-up constants
are wrong, false positives show up there, and it costs a few minutes instead
of invalidating the whole measured baseline.
"""

import json
import subprocess
import sys
import time

NODES = 5
CONTROL_MINUTES = float(__import__("os").getenv("CONTROL_MINUTES", "3"))

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


def sh_bg(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


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


def agent_diagnoses():
    out = sh("docker compose logs --no-color recovery_agent", timeout=120).stdout
    return [l for l in out.splitlines() if "DIAGNOSIS " in l]


def agent_json(path):
    r = sh("docker compose exec -T recovery_agent python -c "
           "\"import urllib.request;print(urllib.request.urlopen"
           "('http://localhost:9600%s').read().decode())\"" % path, timeout=60)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {}


def inject(args, timeout=200):
    return sh("python3 failure_injector/inject.py %s" % args, timeout=timeout)


def main():
    print("=== Phase 5 gate: diagnosis layer (read-only) ===\n")
    if not wait_for_docker():
        return 1

    sh("docker compose down --remove-orphans", timeout=220)
    sh("python3 generate_compose.py --nodes %d" % NODES)
    print("bringing up cluster + read-only agent...")
    if sh("docker compose up -d --build", timeout=1200).returncode != 0:
        print(sh("docker compose logs --tail 40 recovery_agent").stdout[-2000:])
        return 1

    # --------------------------------------------- warm-up barrier is real
    print("\n--- warm-up barrier ---")
    t0 = time.time()
    rc = sh("./scripts/wait_for_warmup.sh", timeout=260).returncode
    waited = time.time() - t0
    check("1. wait_for_warmup blocks then succeeds", rc == 0, "waited %.0fs" % waited)

    # Read-only assertions come AFTER the barrier, not before: `up -d --build`
    # returns as soon as containers are created, so checking the log
    # immediately reads it before the agent has finished starting and reports
    # a healthy agent as broken.
    logs = sh("docker compose logs --no-color recovery_agent", timeout=90).stdout
    # NOT "read-only" any more. Phase 6 gave the agent a policy and it now
    # emits intents, so that assertion became historical the moment Phase 6
    # landed — it failed here for the right reason. What still matters, and
    # always will, is that the agent came up clean and never reads labels
    # (check 0b). Gate 5's job is the DIAGNOSIS baseline, not read-only-ness.
    check("0. agent started cleanly", "[agent]" in logs and "Traceback" not in logs,
          "no startup traceback")
    sub_line = next((l for l in logs.splitlines() if "subscribed to" in l), "")
    check("0b. agent subscribes to health ONLY, never ground truth",
          bool(sub_line) and "cluster.health" in sub_line
          and "cluster.failures" not in sub_line,
          sub_line.split("[agent]")[-1].strip() or "no subscription line")
    check("2. barrier actually waited (not a no-op)", waited > 20,
          "%.0fs — warm-up is 60s/node" % waited)

    # -------------------------------------- CONTROL: no injection, no FPs
    print("\n--- no-injection control (%.0f min) ---" % CONTROL_MINUTES)
    before = len(agent_diagnoses())
    time.sleep(CONTROL_MINUTES * 60)
    after = agent_diagnoses()
    fp = len(after) - before
    check("3. ZERO false positives under steady traffic", fp == 0,
          "%d diagnoses during a fault-free window" % fp)
    if fp:
        for line in after[before:][:5]:
            print("        %s" % line.strip()[-150:])
        note("Control run failed — the measured baseline below is NOT "
             "trustworthy until warm-up/threshold constants are fixed.")

    # ----------------------------------------------------- real injections
    print("\n--- injections ---")
    leader = agent_json("/").get("consensus", {}).get("current_primary")
    voters = [n for n in ("node-1", "node-2", "node-3") if n != leader]
    print("      leader=%s" % leader)

    print("  crash node-4")
    inject("crash --target node-4")
    time.sleep(18)

    print("  cpu burn node-5")
    inject("cpu --target node-5 --workers 2")
    time.sleep(16)

    print("  isolate %s (majority cut)" % voters[0])
    p = sh_bg("python3 failure_injector/inject.py isolate --target %s --duration 35"
              % voters[0])
    time.sleep(22)
    p.wait(timeout=90)

    # 150ms, NOT 400ms. Measured bind (§5.2b): latency is only detectable on
    # the leader (only writes carry netem — Q1), but a delay at or above
    # raftMinTimeout=0.4s triggers an election. The leadership change then
    # invalidates the per-node baseline on BOTH the deposed and the promoted
    # node, so §5.2a correctly makes both abstain and the true positive is
    # lost. 150ms roughly triples the leader's ~150ms write p99 while staying
    # well inside the election timeout, so leadership holds.
    print("  latency +150ms on leader %s (sub-election-timeout)" % leader)
    p = sh_bg("python3 failure_injector/inject.py latency --target %s --ms 150 "
              "--duration 30" % leader)
    time.sleep(20)
    p.wait(timeout=90)

    inject("clear")
    time.sleep(8)

    # ------------------------------------------------------- the numbers
    print("\n--- evaluation ---")
    lines = agent_diagnoses()
    check("4. agent produced diagnoses", len(lines) > 0, "%d total" % len(lines))
    for line in lines[-10:]:
        print("      %s" % line.split("[agent]")[-1].strip()[:150])

    ev = sh("python3 evaluator/evaluate.py --report diagnosis --json", timeout=240)
    print("\n".join(l for l in ev.stdout.splitlines()
                    if not l.startswith("SUMMARY_JSON")))
    if ev.returncode != 0:
        print(ev.stderr[-1200:])
    check("5. evaluator ran", ev.returncode == 0)

    s = {}
    for line in ev.stdout.splitlines():
        if line.startswith("SUMMARY_JSON "):
            s = json.loads(line[len("SUMMARY_JSON "):])
    counts = s.get("counts", {})

    check("6. evaluator used optimal matching (scipy present)",
          s.get("optimal_matching") is True,
          "greedy is order-dependent under overlap (§8.1a)")
    check("7. hyperparameters reported", bool(s.get("hyperparameters")))

    # These three assert on the evaluator's OWN rows. Added because gate run 2
    # reported 9/9 while wrong_target=1 recorded a real false positive — a
    # promoted node's p99 misread as a spike (§5.2a). A number the gate does
    # not assert on is a number nobody acts on.
    check("8. no wrong-target diagnoses", counts.get("wrong_target", 0) == 0,
          "wrong_target=%s" % counts.get("wrong_target"))
    check("9. no false positives", counts.get("false_positive", 0) == 0,
          "false_positive=%s" % counts.get("false_positive"))
    check("10. complete per-type recall", s.get("complete_recall") is True,
          json.dumps(s.get("recall", {})))

    inc = agent_json("/incidents")
    note("incidents: %d open, %d closed — suppression means one line per "
         "incident, not one per second (§5.4)."
         % (len(inc.get("open", [])), inc.get("closed", 0)))

    print("\n" + "=" * 64)
    print("  Phase 5 gate: %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    for n in notes:
        print("    - %s" % n)
    print("=" * 64)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
