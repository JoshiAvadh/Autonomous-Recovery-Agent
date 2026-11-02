"""Policy wiring — diagnosis in, intent out. plan.md Phase 6.

This is the join between the live system and the SIMULATOR'S OWN decision
code. `rule_based.RuleBasedRecovery` is imported unmodified from the repo root
(bind-mounted at /simulator) and called with the RealNetworkAdapter in place of
the SimPy Network.

Phase 6 wires TIER 1 ONLY — the deterministic rule engine, no LLM. That is
deliberate: it demonstrates the whole claim (real telemetry interpreted,
decision made without labels, action executed asynchronously, outcome
independently evaluated) with no model in the loop. Tier 2/3 lands in Phase 7
as an upgrade to the policy layer, not as the thing the demo rests on.

Time here is SECONDS, not the simulator's ticks.
"""

import hashlib
import logging
import time

from recovery_agent.events import from_diagnosis

log = logging.getLogger("policy")

# rule_based.decide() EXECUTES against the network it is given. Our adapter's
# mutators raise, so for Phase 6 we ask only for the SUGGESTION and let the
# executor perform it. get_rule_suggestion() is the non-executing path
# (rule_based.py:336) and exists for exactly this purpose.
TIER1 = 1

# Everything rule_based.get_rule_suggestion() can return, now that Phase 8
# gave the executor a real implementation for each.
#
# `reassign_role` is deliberately absent and is NOT merely unimplemented: under
# real Raft there is no assignable role, so the action has no referent at all
# (§10). get_rule_suggestion never returns it; it exists only in the LLM's
# menu, and removing it there is a Phase 7 concern.
SUPPORTED_ACTIONS = {
    "restart_node", "isolate_node", "monitor_only",
    "reroute_traffic", "rebalance_load", "promote_replica",
    "load_shed", "load_shed_readmit",
}

# promote_replica deliberately creates an unavailability window (it isolates
# the sitting leader to force an election), so its precondition is STRICTER
# than the diagnosis that suggests it — plan.md Phase 8. A destructive action
# should always need more evidence than the observation behind it.
PROMOTE_CONFIRMATIONS = 5
SHED_TIMEOUT_THRESHOLD = 0.50     # mirrors llm_agent.py:34
SHED_READMIT_THRESHOLD = 0.35     # mirrors llm_agent.py:35


def decision_id(incident_id, attempt_no):
    """sha1(incident_id|attempt_no) — §6.1. Anchored on the INCIDENT, so a
    Kafka redelivery collides naturally while genuinely distinct decisions
    (including a follow-up within one incident) never do."""
    raw = "%s|%d" % (incident_id, attempt_no)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


class Policy:
    def __init__(self, adapter, history, rules):
        self.adapter = adapter
        self.history = history
        self.rules = rules
        self._promote_streak = {}
        self._shed = set()

    def _ask_llm(self, event, incident, confidence, tier, reason):
        """Tier 2/3 — hand the decision to llm_agent, UNMODIFIED.

        decide_only() brings the whole apparatus with it: few-shot examples,
        chain-of-thought, decision_memory injection of the top-5 past outcomes,
        and the multi-turn self-critique pass. None of it is reimplemented
        here, which is the entire point of the thesis.

        What this does NOT do is call decide_and_recover() — that executes
        against the network it is given, and our adapter's mutators raise by
        design. The executor performs actions; the policy only decides.
        """
        try:
            from llm_agent import decide_only
        except Exception:
            log.exception("llm_agent import failed")
            return None

        log.info("ROUTED %s | %s on %s | confidence=%d%% -> TIER-%d (%s) "
                 "| asking the model",
                 incident.incident_id, event.failure_type, event.target,
                 confidence, tier, reason)
        try:
            decision = decide_only(self.adapter, event, event.time)
        except Exception:
            log.exception("decide_only failed")
            return None

        action = decision.get("action", "monitor_only")
        target = decision.get("target") or event.target

        # A missing API key makes ask_groq return None, which _parse_llm_json
        # turns into action="none". Degrade visibly rather than silently
        # emitting a no-op intent nobody can account for.
        if action in ("none", "parse_failed", "skipped"):
            log.warning("LLM returned %s for %s — no intent emitted "
                        "(missing GROQ_API_KEY, or an unparseable reply)",
                        action, incident.incident_id)
            return None

        if action not in SUPPORTED_ACTIONS:
            log.info("LLM chose %s, which has no real referent here | no action",
                     action)
            return None

        # The simulator's own action-sanity guard rail. decide_only() does NOT
        # call it — only decide_and_recover() does, and we avoid that one
        # because it executes. Skipping it would mean running a DIFFERENT
        # policy than the simulator's and then claiming the comparison is
        # like-for-like.
        #
        # Measured: the model answered monitor_only for a memory_leak at ~35%
        # health, which validate_action rejects outright
        # (llm_agent.py:232-233). Without this call the live system accepts a
        # decision the simulator would have blocked.
        try:
            from llm_agent import validate_action
            valid, why = validate_action(action, target, self.adapter,
                                         event.failure_type)
        except Exception:
            log.exception("validate_action failed")
            valid, why = True, "validator unavailable"

        if not valid:
            # Faithful to the simulator, which downgrades a rejected action to
            # monitor_only (llm_agent.py:874). NOTE that this is incoherent
            # when monitor_only is itself what was rejected — the fallback is
            # the thing the guard just refused. Reproduced rather than
            # silently improved, because changing it would mean the live
            # system is no longer running the policy under test.
            log.warning("VALIDATOR BLOCKED %s on %s — %s | falling back to "
                        "monitor_only (the simulator's own behaviour, even "
                        "when monitor_only is what was blocked)",
                        action, target, why)
            action = "monitor_only"
            llm_reason = "Blocked: %s" % why
        else:
            llm_reason = decision.get("reason", "")

        log.info("LLM DECIDED %s | %s on %s | %s%s",
                 incident.incident_id, action, target, llm_reason[:90],
                 " [DIVERGES FROM RULE]" if decision.get("diverges_from_rule")
                 else "")
        return action, target, "llm", llm_reason

    def _promote_allowed(self, incident, diagnosis, target):
        """K=5 confirmations AND quorum among the REMAINING voters.

        Forcing a step-down costs real availability: the leader is isolated for
        ~2x raftMaxTimeout before anyone can be elected. Doing that while the
        rest of the cluster could not form a quorum anyway would turn a
        recoverable incident into an outage, so both conditions are required
        and the stricter sample count is the point (plan.md Phase 8).
        """
        key = incident.incident_id
        self._promote_streak[key] = self._promote_streak.get(key, 0) + 1
        seen = self._promote_streak[key]
        if seen < PROMOTE_CONFIRMATIONS:
            log.info("HELD %s | promote_replica needs %d confirmations, have "
                     "%d | no action yet", key, PROMOTE_CONFIRMATIONS, seen)
            return False

        latest = self.history.latest_all()
        leader = self.history.leader()
        remaining = [d for n, d in latest.items()
                     if d.get("role") in ("Primary", "Replica") and n != leader]
        healthy_remaining = [d for d in remaining if d.get("has_quorum")]
        if len(healthy_remaining) < 2:
            log.info("REFUSED %s | promote_replica would isolate the leader "
                     "with only %d healthy voter(s) left — that is an outage, "
                     "not a recovery", key, len(healthy_remaining))
            return False
        return True

    def shed_intents(self, now):
        """Rule-driven load shedding, no LLM.

        DEVIATION from the plan: the simulator's check_load_shedding() asks the
        LLM which node to shed (llm_agent.py:545). Phase 8 has no LLM yet, so
        this uses _rank_shed_candidates() — the pure, deterministic ranking
        function the simulator already has — and takes the top candidate.
        Phase 7 can hand selection back to the model; the safety properties
        (never shed a voter, cap at a fraction of the cluster) live in the
        ranking, not in the model's opinion.
        """
        stats = self.adapter.get_stats()
        try:
            rate = float(str(stats.get("timeout_rate", "0%")).replace("%", "")) / 100.0
        except ValueError:
            return []

        out = []
        if rate > SHED_TIMEOUT_THRESHOLD and not self._shed:
            try:
                from llm_agent import _rank_shed_candidates
                ranked = _rank_shed_candidates(self.adapter)
            except Exception:
                log.exception("shed ranking failed")
                return []
            if ranked:
                victim = ranked[0] if isinstance(ranked[0], str) else ranked[0][0]
                self._shed.add(victim)
                out.append(self._bare_intent("load_shed", victim, now,
                                             "timeout_rate %.0f%%" % (rate * 100)))
        elif rate < SHED_READMIT_THRESHOLD and self._shed:
            victim = sorted(self._shed)[0]
            self._shed.discard(victim)
            out.append(self._bare_intent("load_shed_readmit", victim, now,
                                         "timeout_rate recovered to %.0f%%"
                                         % (rate * 100)))
        return out

    def _bare_intent(self, action, target, now, reason):
        """Shedding is a cluster-level controller decision, not a per-node
        diagnosis, so it gets a synthetic incident id rather than pretending a
        FailureDiagnosis produced it."""
        incident_id = "%s:congestion:%d" % (target, int(now * 1000))
        latest = self.history.latest_all().get(target) or {}
        return {
            "decision_id": decision_id(incident_id, 1),
            "incident_id": incident_id,
            "attempt_no": 1,
            "action": action,
            "target": target,
            "source": "rule_shed",
            "confidence": 100,
            "reason": reason,
            "decided_at": now,
            "expected_target_status": latest.get("status", "healthy"),
            "expected_boot_id": latest.get("boot_id", ""),
            "expected_started_at": float(latest.get("started_at") or 0.0),
            "target_generation": self.history.generation(target),
            "diagnosis": None,
        }

    def intent_for(self, diagnosis, incident):
        """Return a cluster.actions payload, or None to stay silent."""
        event = from_diagnosis(diagnosis, incident)
        now = time.time()

        try:
            confidence, tier, reason = self.rules.assess_confidence(
                self.adapter, event)
        except Exception:
            log.exception("assess_confidence failed for %s", diagnosis)
            return None

        # The rule engine's non-executing suggestion is computed either way:
        # for tier 1 it IS the decision, and for tier 2/3 it is what the LLM's
        # choice is compared against to set diverges_from_rule.
        try:
            rule_action, rule_target = self.rules.get_rule_suggestion(
                self.adapter, event)
        except Exception:
            log.exception("get_rule_suggestion failed for %s", diagnosis)
            return None

        if tier == TIER1:
            action, target, source = rule_action, rule_target, "rule"
            decision_reason = reason
        else:
            decided = self._ask_llm(event, incident, confidence, tier, reason)
            if decided is None:
                return None
            # The intent must record the MODEL's reasoning, not the router's.
            # Carrying the router's confidence rationale would attribute the
            # wrong words to the decision in every audit trail.
            action, target, source, decision_reason = decided
        

        if action not in SUPPORTED_ACTIONS:
            log.info("ROUTED %s | rule chose %s, which has no real referent "
                     "| no action", incident.incident_id, action)
            return None

        if action == "promote_replica" and not self._promote_allowed(
                incident, diagnosis, target):
            return None

        target = target or diagnosis.target
        latest = self.history.latest_all().get(target) or {}
        stale = target in self.history.stale(now)

        attempt = incident.next_attempt()
        did = decision_id(incident.incident_id, attempt)

        return {
            "decision_id": did,
            "incident_id": incident.incident_id,
            "attempt_no": attempt,
            "action": action,
            "target": target,
            "source": source,
            "confidence": int(confidence),
            "reason": decision_reason,
            "router_reason": reason,
            "rule_suggestion": rule_action,
            "decided_at": now,
            # Target-scoped expectations the executor validates against a
            # fresh read (§6.2). boot_id is the identity; status is the premise.
            "expected_target_status": "failed" if stale else latest.get("status", "healthy"),
            "expected_boot_id": latest.get("boot_id", ""),
            "expected_started_at": float(latest.get("started_at") or 0.0),
            # Agent/evaluator metadata only — the executor must ignore it.
            "target_generation": self.history.generation(target),
            "diagnosis": diagnosis.to_dict(),
        }
