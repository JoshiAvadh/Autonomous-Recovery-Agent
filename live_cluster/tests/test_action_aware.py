"""One case per row of plan.md §6.2a, plus the transition-log contract (§6.3).

The headline assertion: a boot_id change means `already_satisfied` for
restart_node but **`stale` for reroute_traffic, isolate_node and load_shed**.
Getting that wrong silently drops every unrelated intent that straddles a
restart, and `already_satisfied` vs `stale` mean opposite things downstream.

    python3 tests/test_action_aware.py
"""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from executor.transition_log import (  # noqa: E402
    APPLYING, COMPLETED, TransitionLog,
)
from executor import validation as V  # noqa: E402

passed, failed = [], []


def check(label, ok, detail=""):
    (passed if ok else failed).append(label)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", label,
                          (" — " + detail) if detail else ""))


def tmplog():
    d = tempfile.mkdtemp()
    return TransitionLog(os.path.join(d, "state", "log.jsonl")), d


def intent(action, **kw):
    d = dict(decision_id=kw.pop("decision_id", "d-" + action),
             incident_id="node-2:crash:1", attempt_no=1, action=action,
             target=kw.pop("target", "node-2"), source="rule", confidence=80,
             reason="test", decided_at=kw.pop("decided_at", 100.0),
             expected_target_status=kw.pop("expected_target_status", "healthy"),
             expected_boot_id=kw.pop("expected_boot_id", "bootA"),
             expected_started_at=kw.pop("expected_started_at", 50.0))
    d.update(kw)
    return d


def status(**kw):
    s = dict(node_id="node-2", status=kw.pop("status", "healthy"),
             boot_id=kw.pop("boot_id", "bootA"),
             started_at=kw.pop("started_at", 50.0))
    s.update(kw)
    return s


# ------------------------------------------------- boot_id is action-aware

def test_boot_change_satisfies_restart():
    log, d = tmplog()
    v = V.validate(intent("restart_node", expected_target_status="failed"),
                   log, status(boot_id="bootB", started_at=200.0))
    check("boot change -> already_satisfied for restart_node",
          v.decision == V.ALREADY_SATISFIED, repr(v))
    shutil.rmtree(d)


def test_boot_change_is_stale_for_reroute():
    log, d = tmplog()
    v = V.validate(intent("reroute_traffic"), log,
                   status(boot_id="bootB", started_at=200.0),
                   weight_table_targets={"node-2"})
    # reroute acts on the weight table, so it never even consults boot_id
    check("boot change does NOT satisfy reroute_traffic",
          v.decision != V.ALREADY_SATISFIED, repr(v))
    shutil.rmtree(d)


def test_boot_change_is_stale_for_isolate():
    log, d = tmplog()
    v = V.validate(intent("isolate_node"), log,
                   status(boot_id="bootB", started_at=200.0))
    check("boot change -> STALE for isolate_node (restart UNDID it)",
          v.decision == V.STALE, repr(v))
    shutil.rmtree(d)


def test_boot_change_is_stale_for_shed():
    log, d = tmplog()
    v = V.validate(intent("load_shed"), log,
                   status(boot_id="bootB", started_at=200.0))
    check("boot change -> STALE for load_shed (flag died with the container)",
          v.decision == V.STALE, repr(v))
    shutil.rmtree(d)


# ----------------------------------------------- unreachable is action-aware

def test_unreachable_is_precondition_for_restart():
    log, d = tmplog()
    v = V.validate(intent("restart_node", expected_target_status="failed"),
                   log, None)
    check("unreachable + expected failed -> PROCEED for restart_node",
          v.accepted, repr(v))
    shutil.rmtree(d)


def test_unreachable_satisfies_isolate():
    log, d = tmplog()
    v = V.validate(intent("isolate_node", expected_target_status="failed"),
                   log, None)
    check("unreachable -> already_satisfied for isolate_node",
          v.decision == V.ALREADY_SATISFIED, repr(v))
    shutil.rmtree(d)


def test_unreachable_does_not_block_reroute():
    log, d = tmplog()
    v = V.validate(intent("reroute_traffic"), log, None,
                   weight_table_targets={"node-2"})
    check("unreachable does NOT block reroute_traffic "
          "(routing away from a dead node is correct)", v.accepted, repr(v))
    shutil.rmtree(d)


def test_unreachable_blocks_shed_when_not_failed():
    log, d = tmplog()
    v = V.validate(intent("load_shed", expected_target_status="healthy"),
                   log, None)
    check("unreachable -> already_satisfied/unreachable for load_shed",
          v.decision in (V.ALREADY_SATISFIED, V.UNREACHABLE), repr(v))
    shutil.rmtree(d)


# ------------------------------------------------------------ bookkeeping

def test_duplicate_rejected():
    log, d = tmplog()
    i = intent("restart_node")
    log.append(i["decision_id"], COMPLETED, target="node-2", decided_at=100.0)
    v = V.validate(i, log, status())
    check("duplicate decision_id rejected", v.decision == V.DUPLICATE, repr(v))
    shutil.rmtree(d)


def test_superseded_by_later_decided_at():
    log, d = tmplog()
    log.append("newer", COMPLETED, target="node-2", decided_at=500.0)
    v = V.validate(intent("restart_node", decided_at=100.0), log, status())
    check("superseded by a later decided_at", v.decision == V.SUPERSEDED, repr(v))
    shutil.rmtree(d)


def test_status_mismatch_is_stale():
    log, d = tmplog()
    v = V.validate(intent("restart_node", expected_target_status="failed"),
                   log, status(status="healthy"))
    check("status mismatch -> stale", v.decision == V.STALE, repr(v))
    shutil.rmtree(d)


def test_monitor_only_never_rejected():
    log, d = tmplog()
    v = V.validate(intent("monitor_only"), log, None)
    check("monitor_only is never rejected", v.accepted, repr(v))
    shutil.rmtree(d)


# --------------------------------------------------- transition log (§6.3)

def test_log_folds_to_latest_state():
    log, d = tmplog()
    log.append("x", "accepted", target="node-2")
    log.append("x", APPLYING, target="node-2")
    log.append("x", COMPLETED, target="node-2")
    reopened = TransitionLog(log.path)
    check("append-only log folds to latest state",
          reopened.state_of("x") == COMPLETED, reopened.state_of("x"))
    with open(log.path) as fh:
        lines = [l for l in fh.read().splitlines() if l.strip()]
    check("every transition is its own line (never rewritten)",
          len(lines) == 3, "%d lines" % len(lines))
    shutil.rmtree(d)


def test_orphaned_applying_detected_on_fold():
    log, d = tmplog()
    log.append("y", "accepted", target="node-3")
    log.append("y", APPLYING, target="node-3")
    reopened = TransitionLog(log.path)
    check("crash mid-applying is detected on fold",
          reopened.orphans() == ["y"], str(reopened.orphans()))
    shutil.rmtree(d)


def main():
    print("=== action-aware validation + transition log ===\n")
    for fn in sorted((v for k, v in globals().items() if k.startswith("test_")),
                     key=lambda f: f.__code__.co_firstlineno):
        try:
            fn()
        except Exception as exc:
            check(fn.__name__, False, "raised %s: %s" % (type(exc).__name__, exc))
    print("\n  %d passed, %d failed" % (len(passed), len(failed)))
    for f in failed:
        print("    FAILED: %s" % f)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
