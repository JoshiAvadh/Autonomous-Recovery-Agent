#!/usr/bin/env python3
"""Offline evaluator. plan.md §8.1a.

The ONLY consumer of cluster.failures. Joins unlabeled diagnoses to ground
truth after the fact, so the agent never sees a label.

Matching is a globally optimal min-cost assignment, not greedy nearest-first:
greedy is order-dependent the moment incidents overlap, and the reported
accuracy would then depend on arrival order rather than classifier quality.

The cost matrix is PADDED WITH DUMMIES. Without them linear_sum_assignment
produces a MAXIMUM matching — every diagnosis forced onto some incident —
which would fabricate wrong-type matches and drive false positives and missed
incidents to zero by construction.

Runs on the host. scipy is evaluator-only and never ships in a node image.
"""

import argparse
import json
import subprocess
import sys

try:
    from scipy.optimize import linear_sum_assignment
    HAVE_SCIPY = True
except ImportError:
    HAVE_SCIPY = False

# All four are EVALUATION HYPERPARAMETERS and are reported with every result.
# The first two decide how readily a cascade neighbour is attributed to an
# incident; the last two set the price of declaring a false positive or a miss
# rather than accepting a poor match. Together they move every column, so an
# unreported value makes the numbers unreproducible.
TARGET_PENALTY = 20.0
TYPE_PENALTY = 15.0
UNMATCHED_D = 25.0
UNMATCHED_G = 25.0
W_MAX = 30.0
# Must exceed UNMATCHED_D + UNMATCHED_G so the solver always prefers a dummy
# over an infeasible pairing. Finite, not inf: scipy raises on matrices that
# are infeasible-infinite.
BIG = 1e6


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def read_topic(topic, seconds=8, max_msgs=2000):
    """cluster.failures and the agent log are HISTORICAL — always from the
    beginning. Reading them from latest returns nothing."""
    r = sh("docker compose exec -T kafka timeout %d "
           "/opt/kafka/bin/kafka-console-consumer.sh "
           "--bootstrap-server localhost:9092 --topic %s --from-beginning "
           "--max-messages %d --timeout-ms %d 2>/dev/null"
           % (seconds + 12, topic, max_msgs, seconds * 1000),
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


def load_ground_truth():
    """injected + resolved records, joined by gt_id."""
    rows = read_topic("cluster.failures")
    gt = {}
    for r in rows:
        gid = r.get("gt_id")
        if not gid:
            continue
        if r.get("event") == "injected":
            gt.setdefault(gid, {}).update(
                gt_id=gid, failure_type=r.get("failure_type"),
                target=r.get("target"), injected_at=r.get("injected_at"),
                duration_hint=r.get("duration_hint"),
                resolved_at=None, resolved_by="unresolved")
        elif r.get("event") == "resolved":
            gt.setdefault(gid, {}).update(
                resolved_at=r.get("resolved_at"),
                resolved_by=r.get("resolved_by", "injector"))
    return [g for g in gt.values() if g.get("injected_at")]


def load_diagnoses(agent_log_path=None):
    """Parse DIAGNOSIS lines out of the agent's log.

    Phase 5 logs rather than publishes: the agent takes no action, so there is
    no cluster.actions stream to read yet.
    """
    if agent_log_path:
        with open(agent_log_path) as fh:
            lines = fh.read().splitlines()
    else:
        lines = sh("docker compose logs --no-color recovery_agent",
                   timeout=90).stdout.splitlines()

    out = []
    for line in lines:
        if "DIAGNOSIS " not in line:
            continue
        try:
            body = line.split("DIAGNOSIS ", 1)[1]
            parts = [p.strip() for p in body.split("|")]
            incident_id = parts[0]
            what, _, target = parts[1].partition(" on ")
            conf_part = parts[2]
            ambiguous = "AMBIGUOUS" in conf_part
            conf = float(conf_part.split("conf=")[1].split()[0])
            # incident_id is target:type:opened_ms -> recover the timestamp
            observed_at = int(incident_id.rsplit(":", 1)[1]) / 1000.0
            out.append(dict(incident_id=incident_id, failure_type=what.strip(),
                            target=target.strip(), confidence=conf,
                            ambiguous=ambiguous, observed_at=observed_at,
                            evidence=parts[3] if len(parts) > 3 else ""))
        except (IndexError, ValueError):
            continue
    return out


def feasible(d, g):
    """Window ends at resolved_at when known — NEVER duration_hint, which is
    what the injector intended, not what happened (§4.2)."""
    start = g["injected_at"]
    end = g.get("resolved_at") or (start + W_MAX)
    end = min(end, start + W_MAX)
    return start <= d["observed_at"] <= end


def cost(d, g):
    c = abs(d["observed_at"] - g["injected_at"])
    if d["target"] != g["target"]:
        c += TARGET_PENALTY
    if d["failure_type"] != g["failure_type"]:
        c += TYPE_PENALTY
    return c


def match(diagnoses, truths):
    """Optimal one-to-one with dummy rows/columns so things CAN go unmatched."""
    nd, ng = len(diagnoses), len(truths)
    if nd == 0 and ng == 0:
        return [], [], []
    size = nd + ng
    m = [[BIG] * size for _ in range(size)]

    for i, d in enumerate(diagnoses):
        for j, g in enumerate(truths):
            m[i][j] = cost(d, g) if feasible(d, g) else BIG
        for j in range(ng, size):
            m[i][j] = UNMATCHED_D if (j - ng) == i else BIG
    for i in range(nd, size):
        for j in range(ng):
            m[i][j] = UNMATCHED_G if (i - nd) == j else BIG
        for j in range(ng, size):
            m[i][j] = 0.0

    if HAVE_SCIPY:
        rows, cols = linear_sum_assignment(m)
        pairs = list(zip(rows, cols))
    else:
        # Greedy fallback, clearly labelled. Order-dependent under overlap —
        # exactly the defect §8.1a exists to avoid — so the report says so.
        pairs, used = [], set()
        order = sorted(((m[i][j], i, j) for i in range(size) for j in range(size)),
                       key=lambda t: t[0])
        taken_r, taken_c = set(), set()
        for _, i, j in order:
            if i in taken_r or j in taken_c:
                continue
            taken_r.add(i)
            taken_c.add(j)
            pairs.append((i, j))
        _ = used

    matched, unmatched_d, unmatched_g = [], [], []
    for i, j in pairs:
        if i < nd and j < ng and m[i][j] < BIG:
            matched.append((diagnoses[i], truths[j]))
        elif i < nd <= j:
            unmatched_d.append(diagnoses[i])
        elif j < ng <= i:
            unmatched_g.append(truths[j])
    return matched, unmatched_d, unmatched_g


def classify_outcomes(matched, unmatched_d, unmatched_g):
    rows = {"true_positive": [], "wrong_type": [], "wrong_target": [],
            "ambiguous_right_target": [], "ambiguous_wrong_target": [],
            "false_positive": [], "missed": []}
    for d, g in matched:
        same_target = d["target"] == g["target"]
        if d["ambiguous"]:
            rows["ambiguous_right_target" if same_target
                 else "ambiguous_wrong_target"].append((d, g))
        elif not same_target:
            # A cascade neighbour, not a hallucination. Counting it as a false
            # positive would erase the difference between "invented a failure"
            # and "saw the blast radius but misattributed the cause".
            rows["wrong_target"].append((d, g))
        elif d["failure_type"] != g["failure_type"]:
            rows["wrong_type"].append((d, g))
        else:
            rows["true_positive"].append((d, g))
    rows["false_positive"] = list(unmatched_d)
    rows["missed"] = list(unmatched_g)
    return rows


def report(rows, truths, diagnoses):
    print("\n" + "=" * 66)
    print("  DIAGNOSIS EVALUATION  (plan.md §8.1a)")
    print("=" * 66)
    print("  ground-truth incidents : %d" % len(truths))
    print("  diagnoses emitted      : %d" % len(diagnoses))
    if not HAVE_SCIPY:
        print("  !! scipy missing — GREEDY fallback used; order-dependent "
              "under overlapping incidents. Install scipy for the real number.")
    print()
    for k in ("true_positive", "wrong_type", "wrong_target",
              "ambiguous_right_target", "ambiguous_wrong_target",
              "false_positive", "missed"):
        print("  %-24s %d" % (k, len(rows[k])))

    tp = rows["true_positive"]
    if tp:
        lat = sorted(d["observed_at"] - g["injected_at"] for d, g in tp)
        med = lat[len(lat) // 2]
        print("\n  detection latency over TRUE POSITIVES only "
              "(including wrong-type would conflate speed with correctness):")
        print("    median %.1fs   min %.1fs   max %.1fs" % (med, lat[0], lat[-1]))
        print("    NOTE: §5.2 hysteresis puts a ~3s floor under this BY DESIGN.")

    by_type = {}
    for d, g in tp:
        by_type[g["failure_type"]] = by_type.get(g["failure_type"], 0) + 1
    counts = {}
    for g in truths:
        counts[g["failure_type"]] = counts.get(g["failure_type"], 0) + 1
    if counts:
        print("\n  per-type recall:")
        for t in sorted(counts):
            print("    %-18s %d/%d" % (t, by_type.get(t, 0), counts[t]))

    print("\n  hyperparameters (required for reproducibility, §8.1a):")
    print("    TARGET_PENALTY=%s  TYPE_PENALTY=%s  UNMATCHED_D=%s  "
          "UNMATCHED_G=%s  W_MAX=%ss"
          % (TARGET_PENALTY, TYPE_PENALTY, UNMATCHED_D, UNMATCHED_G, W_MAX))
    print("=" * 66)


def summary(rows, truths, diagnoses):
    """Machine-readable counts, so a gate can ASSERT instead of eyeballing.

    Phase 5 gate run 2 reported 9/9 while the evaluator's own wrong_target row
    recorded a real false positive. Prose the gate does not read is prose
    nobody acts on.
    """
    recall = {}
    for g in truths:
        t = g["failure_type"]
        r = recall.setdefault(t, {"hit": 0, "total": 0})
        r["total"] += 1
    for d, g in rows["true_positive"]:
        recall[g["failure_type"]]["hit"] += 1
    return {
        "ground_truth": len(truths),
        "diagnoses": len(diagnoses),
        "counts": {k: len(v) for k, v in rows.items()},
        "recall": recall,
        "complete_recall": all(r["hit"] == r["total"] for r in recall.values()),
        "optimal_matching": HAVE_SCIPY,
        "hyperparameters": {
            "TARGET_PENALTY": TARGET_PENALTY, "TYPE_PENALTY": TYPE_PENALTY,
            "UNMATCHED_D": UNMATCHED_D, "UNMATCHED_G": UNMATCHED_G,
            "W_MAX": W_MAX,
        },
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", default="diagnosis")
    ap.add_argument("--agent-log", default=None,
                    help="read diagnoses from a file instead of docker logs")
    ap.add_argument("--json", action="store_true",
                    help="emit a machine-readable summary for gate assertions")
    args = ap.parse_args()

    truths = load_ground_truth()
    diagnoses = load_diagnoses(args.agent_log)
    matched, ud, ug = match(diagnoses, truths)
    rows = classify_outcomes(matched, ud, ug)
    if args.json:
        print("SUMMARY_JSON " + json.dumps(summary(rows, truths, diagnoses)))
    report(rows, truths, diagnoses)
    return 0


if __name__ == "__main__":
    sys.exit(main())
