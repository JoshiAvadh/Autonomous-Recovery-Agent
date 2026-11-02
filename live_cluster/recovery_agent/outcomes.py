"""Outcome tracking: scoring, feedback, cross-run memory. plan.md Phase 7.

Closes the loop. The agent emits an intent, the executor reports what happened,
and this module turns that into a score, a success/failure verdict, and a
persisted memory the next run's prompts will read.

Three deliberate choices:

1. SCORING TIMING IS SECONDS, NOT TICKS. scorer.py's `score_after = t + 5.0`
   means five seconds here. That is the intended reading, and it is also
   exactly why absolute scores do not transfer between the simulator and this
   system (§8.3) — the same constant means different things.

2. PER-ACTION FEEDBACK WINDOWS. feedback.py hardcodes 5 ([feedback.py:51](feedback.py#L51))
   and counts restart_node successful only at health >= 70. A real container at
   t+5s is often still booting and catching up its Raft log, so it reads as
   failed -> follow-up -> another restart -> oscillation. The per-action table
   below owns the TIMING while still reusing feedback.evaluate_action_result()
   for the per-action success CRITERIA, which is the valuable part.

3. SCORES STAY INTERNAL. They are not a valid sim-vs-real comparison metric
   (§8.3), but decision_memory.get_relevant_context() sorts by them
   ([decision_memory.py:85](decision_memory.py#L85)) to pick the top-5 outcomes
   injected into future prompts. Dropping them would silently disable
   cross-run learning.
"""

import logging
import threading
import time

log = logging.getLogger("outcomes")

# How long to wait before judging each action. Measured against real container
# behaviour, not the simulator's uniform 5 ticks.
FEEDBACK_WINDOW_S = {
    "reroute_traffic": 3.0,
    "rebalance_load": 3.0,
    "isolate_node": 5.0,
    "load_shed": 8.0,
    "load_shed_readmit": 8.0,
    "promote_replica": 10.0,
    "restart_node": 15.0,      # boot + Raft catch-up
    "ENSURE_RUNNING": 15.0,
    "monitor_only": 5.0,
}
DEFAULT_WINDOW_S = 8.0
SCORE_AFTER_S = 5.0


class OutcomeTracker:
    def __init__(self, history, scorer, memory, evaluate_action_result,
                 publish_score=None):
        self.h = history
        self.scorer = scorer
        self.memory = memory
        self._evaluate = evaluate_action_result
        # §4.2 lists cluster.scores as a real topic the evaluator consumes.
        # It was specified, the constant was imported, and nothing ever
        # published to it — the gate "decisions are scored" passed by matching
        # a LOG LINE while the topic stayed empty.
        self._publish_score = publish_score
        self._lock = threading.Lock()
        self._pending_score = {}      # decision_id -> (scorer_id, event, due)
        self._pending_feedback = {}   # decision_id -> dict
        self._scored = 0
        self._followups = 0

    # ------------------------------------------------- at decision time

    def register(self, intent, event, adapter):
        """Snapshot pre-state so the score has something to compare against."""
        try:
            sid = self.scorer.register_decision(
                current_time=intent["decided_at"],
                action=intent["action"],
                target=intent["target"],
                source=intent["source"],
                failure_type=(intent.get("diagnosis") or {}).get(
                    "failure_type", "unknown"),
                network=adapter,
                rule_suggestion=intent.get("rule_suggestion"),
            )
        except Exception:
            log.exception("scorer.register_decision failed")
            return
        with self._lock:
            self._pending_score[intent["decision_id"]] = {
                "scorer_id": sid, "event": event, "intent": intent,
                "due": intent["decided_at"] + SCORE_AFTER_S,
            }

    # -------------------------------------------- when the executor reports

    def on_result(self, result):
        """cluster.action_results arrived. Only terminal states schedule a
        feedback check — an `accepted` or `applying` tells us nothing about
        whether the action worked (§6.3)."""
        state = result.get("state")
        did = result.get("decision_id")
        if state not in ("completed", "failed"):
            return
        with self._lock:
            pend = self._pending_score.get(did)
            # MARK THE EVENT RESOLVED. scorer.score_outcome gives
            # speed_score = 0 outright when resolved is False (scorer.py:78-80),
            # and speed is 35% of the total. LiveFailureEvent.resolve() existed
            # and was never called by anything, so EVERY live score was
            # understated by up to 35 points — and decision_memory sorts by
            # score to choose which outcomes to inject into future prompts, so
            # the distortion fed straight back into the policy.
            #
            # A completed executor action is the signal: the recovery ran. The
            # feedback check below still judges whether it WORKED, which is a
            # separate question from whether it was carried out.
            if pend and state == "completed":
                ev = pend.get("event")
                if ev is not None and not ev.resolved:
                    ev.resolve(time.time())
            intent = pend["intent"] if pend else None
            if intent is None:
                return      # not ours (e.g. a gate-injected intent)
            action = intent["action"]
            self._pending_feedback[did] = {
                "intent": intent,
                "executor_state": state,
                "due": time.time() + FEEDBACK_WINDOW_S.get(
                    action, DEFAULT_WINDOW_S),
            }

    # ------------------------------------------------------- periodic tick

    def tick(self, adapter, now=None):
        """Score anything due, judge anything due. Returns follow-up intents."""
        now = now if now is not None else time.time()
        self._score_due(adapter, now)
        return self._judge_due(now)

    def _score_due(self, adapter, now):
        with self._lock:
            due = [(d, p) for d, p in self._pending_score.items() if now >= p["due"]]
        for did, pend in due:
            event = pend["event"]
            try:
                rec = self.scorer.score_outcome(
                    pend["scorer_id"], now, adapter,
                    resolved=getattr(event, "resolved", False))
            except Exception:
                log.exception("score_outcome failed for %s", did)
                rec = None
            with self._lock:
                self._pending_score.pop(did, None)
            if not rec:
                continue
            self._scored += 1
            self._persist(rec, adapter, pend["intent"])
            if self._publish_score:
                payload = dict(rec)
                payload["decision_id"] = did
                payload["incident_id"] = pend["intent"].get("incident_id")
                self._publish_score(payload)
            self.last_score = rec
            log.info("SCORE %s | %s on %s | total=%.0f "
                     "(speed=%.0f stability=%.0f cascade=%.0f delivery=%.0f) "
                     "source=%s",
                     did, rec["action"], rec["target"], rec["total_score"],
                     rec["speed_score"], rec["stability_score"],
                     rec["cascade_score"], rec["delivery_score"], rec["source"])

    def _persist(self, rec, adapter, intent):
        """Write the outcome to decision_history.json for cross-run learning."""
        nodes = adapter.nodes
        node = nodes.get(rec["target"])
        concurrent = sum(1 for n in nodes.values()
                         if n.status in ("failed", "degraded"))
        try:
            self.memory.record_outcome(
                failure_type=rec.get("failure_type", "unknown"),
                node_role=node.role if node else "unknown",
                action=rec["action"], source=rec["source"],
                score=rec["total_score"],
                resolved=rec.get("resolved", False),
                mttr=rec.get("mttr"),
                cluster_size=len(nodes),
                concurrent_failures=concurrent,
                health_at_decision=node.health if node else 0,
            )
            self.memory.save()
        except Exception:
            log.exception("memory.record_outcome failed")

    def _judge_due(self, now):
        with self._lock:
            due = [(d, p) for d, p in self._pending_feedback.items()
                   if now >= p["due"]]
            for d, _ in due:
                self._pending_feedback.pop(d, None)

        followups = []
        for did, pend in due:
            intent = pend["intent"]
            target = intent["target"]
            node = self.h.latest_all().get(target)
            if node is None:
                log.info("FEEDBACK %s | %s on %s | target silent — cannot judge",
                         did, intent["action"], target)
                continue

            # Reuse the simulator's per-action success CRITERIA; the timing is
            # ours. evaluate_action_result wants an object with .role/.status/
            # .health/.load, which NodeView already satisfies.
            view = _NodeLike(node)
            try:
                ok, reason = self._evaluate(view, intent["action"])
            except Exception:
                log.exception("evaluate_action_result failed")
                continue

            log.info("FEEDBACK %s | %s on %s after %.0fs | %s — %s",
                     did, intent["action"], target,
                     FEEDBACK_WINDOW_S.get(intent["action"], DEFAULT_WINDOW_S),
                     "SUCCEEDED" if ok else "DID NOT WORK", reason)
            if not ok:
                self._followups += 1
                followups.append((intent, reason))
        return followups

    def stats(self):
        return {"scored": self._scored, "followups": self._followups,
                "pending_score": len(self._pending_score),
                "pending_feedback": len(self._pending_feedback)}


class _NodeLike:
    """Minimal shim for feedback.evaluate_action_result()."""

    def __init__(self, msg):
        self.id = msg.get("node_id")
        self.role = msg.get("role", "Worker")
        self.status = msg.get("status", "healthy")
        self.health = float(msg.get("health", 0.0))
        self.load = float(msg.get("load", 0.0))
