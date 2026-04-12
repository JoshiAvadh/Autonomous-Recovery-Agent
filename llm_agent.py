from groq import Groq
import json
import threading

from config import (
    CACHE_MAX_SIZE,
    COOLDOWN_TICKS,
    GROQ_API_KEY,
    LLM_MAX_TOKENS,
    LLM_MODEL,
    LLM_TEMPERATURE,
)
from decision_memory import memory as decision_memory

client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


_action_history = []


_last_action_time = {}


_decision_cache = {}


_llm_overrides = 0
_rule_echoes   = 0

_pending_restarts = {}
_restart_lock     = threading.Lock()


SHED_TIMEOUT_THRESHOLD    = 0.50   
READMIT_TIMEOUT_THRESHOLD = 0.35   
MAX_SHED_FRACTION         = 0.20   

_shed_nodes      = set()
_shedding_active = False
_shed_lock       = threading.Lock()

RULE_DEFAULTS = {
    "crash":              "restart_node",
    "network_partition":  "rebalance_load",
    "memory_leak":        "isolate_node",      
    "memory_leak_early":  "restart_node",      
    "latency_spike":      "rebalance_load",
    "cpu_overload":       "reroute_traffic",
}


CONSENSUS_ROLES = {"Primary", "Replica"}


def get_metrics():
    return {
        "llm_overrides":   _llm_overrides,
        "rule_echoes":     _rule_echoes,
        "total_decisions": len(_action_history),
        "shed_nodes":      list(_shed_nodes),
        "shedding_active": _shedding_active,
    }


def record_action(time, action, target, result, source="llm"):
    _action_history.append({
        "time":   time,
        "action": action,
        "target": target,
        "result": result,
        "source": source,
    })


def _make_cache_key(failure_event, network):
    node = network.nodes.get(failure_event.target)
    stats = network.get_stats() if hasattr(network, "get_stats") else {}

    role = node.role if node else "unknown"
    health_bucket = (int(node.health) // 10) * 10 if node else 0
    load_bucket = (int(node.load) // 10) * 10 if node else 0

    active_failures = sum(
        1 for n in network.nodes.values()
        if n.status in ("failed", "degraded")
    )
    active_failures_bucket = min(active_failures, 4)

    timeout_raw = str(stats.get("timeout_rate", "0%")).replace("%", "")
    try:
        timeout_bucket = (int(float(timeout_raw)) // 10) * 10
    except Exception:
        timeout_bucket = 0

    criticality = "critical" if role in ("Primary", "Replica") else (
        "important" if role == "Gateway" else "normal"
    )

    return (
        failure_event.failure_type,
        role,
        criticality,
        health_bucket,
        load_bucket,
        active_failures_bucket,
        timeout_bucket,
        bool(_shedding_active),
    )


def ask_groq(prompt, system_prompt=None, max_tokens=None):
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    if client is None:
        print("[LLM ERROR] GROQ_API_KEY is not configured")
        return None

    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            max_tokens=max_tokens or LLM_MAX_TOKENS,
            temperature=LLM_TEMPERATURE,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"[LLM ERROR] {e}")
        return None


def get_cluster_snapshot(network):
    lines = []
    consensus = network.consensus_snapshot() if hasattr(network, "consensus_snapshot") else {}
    if consensus:
        lines.append(
            "Consensus: "
            f"primary={consensus.get('current_primary')}, "
            f"quorum={'yes' if consensus.get('has_quorum') else 'no'}, "
            f"commit_index={consensus.get('commit_index', 0)}, "
            f"leader_elections={consensus.get('leader_elections', 0)}"
        )
    stats = network.get_stats() if hasattr(network, "get_stats") else {}
    if stats:
        lines.append(
            "Correctness: "
            f"commit_rate={stats.get('commit_rate', '0%')}, "
            f"stale_read_rate={stats.get('stale_read_rate', '0%')}, "
            f"timeout_rate={stats.get('timeout_rate', '0%')}"
        )

    
    if _shed_nodes:
        lines.append(f"LOAD SHEDDING ACTIVE: shed nodes = {sorted(_shed_nodes)}")

    for node_id, node in network.nodes.items():
        lag = getattr(node, "replica_lag", None)
        queue = getattr(node, "queue_length", 0)
        lag_text = f", replica_lag={lag}" if lag is not None else ""
        shed_tag = " [SHED]" if node_id in _shed_nodes else ""
        lines.append(
            f"{node_id} ({node.role}): status={node.status}, "
            f"health={node.health:.0f}%, load={node.load:.0f}%, queue={queue}{lag_text}{shed_tag}"
        )
    return "\n".join(lines)


def get_recent_history(limit=5):
    if not _action_history:
        return "No previous actions."
    recent = _action_history[-limit:]
    return "\n".join([
        f"t={h['time']}: {h['action']} on {h['target']} -> {h['result']}"
        for h in recent
    ])


def _parse_llm_json(raw, fallback_target):
    if not raw:
        return {
            "action": "none", "target": fallback_target,
            "reason": "LLM unavailable", "urgency": "low",
            "confidence": 0, "diverges_from_rule": False,
        }
    try:
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception:
        return {
            "action": "parse_failed", "target": fallback_target,
            "reason": "Could not parse LLM response", "urgency": "low",
            "confidence": 0, "diverges_from_rule": False,
        }


def _normalize_target(target, network, fallback_target):
    if target == "network":
        healthy = [n for n in network.nodes.values() if n.status == "healthy"]
        if healthy:
            return max(healthy, key=lambda n: n.load).id
        return fallback_target
    return target


def validate_action(action, target, network, failure_type):
    node = network.nodes.get(target)
    stats = network.get_stats() if hasattr(network, "get_stats") else {}
    timeout_rate = float(str(stats.get("timeout_rate", "0%")).replace("%", "") or 0)

    active_failures = sum(
        1 for n in network.nodes.values()
        if n.status in ("failed", "degraded")
    )

    if action == "monitor_only":
        if node and node.status == "failed":
            return False, "Cannot monitor_only a failed node"
        if node and node.role in ("Primary", "Replica"):
            return False, "Cannot monitor_only a critical consensus node"
        if _shedding_active:
            return False, "Cannot monitor_only during load shedding"
        if active_failures >= 2:
            return False, "Cannot monitor_only with multiple active failures"
        if failure_type in ("crash", "cpu_overload"):
            return False, "Cannot monitor_only for this failure type"
        if failure_type in ("memory_leak", "memory_leak_early") and node and node.health < 60:
            return False, "Cannot monitor_only a memory leak with health below 60%"
        if timeout_rate >= 40:
            return False, "Cannot monitor_only under severe congestion"

    return True, "Action valid"


def _build_situational_context(network, failure_event):
    """
    IMPROVEMENT: Richer context including:
    - Count of concurrent active failures (cascade risk signal)
    - Which nodes are degraded (not just failed)
    - Explicit cascade risk warning when multiple failures co-occur
    - Clear divergence opportunity for memory_leak with health >= 20%
    - Load shedding status and congestion metrics
    """
    node = network.nodes.get(failure_event.target)
    healthy_nodes  = [n for n in network.nodes.values() if n.status == "healthy"]
    degraded_nodes = [n for n in network.nodes.values() if n.status == "degraded"]
    failed_nodes   = [n for n in network.nodes.values() if n.status == "failed"]

    primary_count = sum(1 for n in healthy_nodes if n.role == "Primary")
    replica_count = sum(1 for n in healthy_nodes if n.role == "Replica")

    lines = [
        f"Cluster: {len(healthy_nodes)} healthy, {len(degraded_nodes)} degraded, {len(failed_nodes)} failed",
        f"Healthy Primaries: {primary_count}, Healthy Replicas: {replica_count}",
    ]
    if degraded_nodes:
        dnames = ", ".join(f"{n.id}({n.health:.0f}%)" for n in degraded_nodes)
        lines.append(f"Degraded nodes: {dnames}")

    if node:
        lines.append(
            f"Affected: {failure_event.target} ({node.role}), "
            f"health={node.health:.0f}%, load={node.load:.0f}%, status={node.status}"
        )

    if healthy_nodes:
        avg_load = sum(n.load for n in healthy_nodes) / len(healthy_nodes)
        max_node = max(healthy_nodes, key=lambda n: n.load)
        lines.append(f"Avg load: {avg_load:.0f}%, peak: {max_node.id} at {max_node.load:.0f}%")

    
    concurrent_failures = len(degraded_nodes) + len(failed_nodes)
    if concurrent_failures >= 2:
        lines.append(
            f"*** CASCADE RISK: {concurrent_failures} nodes degraded/failed simultaneously. "
            f"Prioritise cluster stability over individual node recovery. ***"
        )

    
    if _shedding_active:
        lines.append(
            f"*** LOAD SHEDDING MODE ACTIVE: {len(_shed_nodes)} nodes shed "
            f"({sorted(_shed_nodes)}). Do not route traffic to shed nodes. ***"
        )

    
    stats = network.get_stats() if hasattr(network, "get_stats") else {}
    if stats:
        timeout_rate_str = stats.get("timeout_rate", "0%")
        try:
            timeout_pct = float(timeout_rate_str.replace("%", ""))
        except (ValueError, AttributeError):
            timeout_pct = 0.0
        if timeout_pct > 30:
            lines.append(
                f"*** CONGESTION WARNING: timeout_rate={timeout_rate_str}. "
                f"Cluster may need load shedding to restore commit throughput. ***"
            )

    
    rule_action = RULE_DEFAULTS.get(failure_event.failure_type, "monitor_only")
    lines.append(f"Default rule action for {failure_event.failure_type}: {rule_action}")
    return "\n".join(lines)




def _get_timeout_rate(network):
    """Extract the current timeout rate as a float (0.0–1.0)."""
    stats = network.get_stats() if hasattr(network, "get_stats") else {}
    raw = stats.get("timeout_rate", "0%")
    try:
        # Handle both "72.5%" and 0.725 formats
        if isinstance(raw, str):
            return float(raw.replace("%", "")) / 100.0
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def _rank_shed_candidates(network):
    """
    Rank nodes by shedding priority (best candidates first).
    NEVER sheds Primary or Replica nodes — consensus must be protected.

    Ranking criteria (worst-first = shed-first):
      1. Already degraded or low health → shed first
      2. Higher load → shedding frees more capacity
      3. Workers before Gateways (gateways handle client connections)
    """
    candidates = []
    for nid, node in network.nodes.items():
        if node.role in CONSENSUS_ROLES:
            continue  
        if node.status == "failed":
            continue  
        if nid in _shed_nodes:
            continue  

        
        score = 0
        if node.status == "degraded":
            score += 50
        score += (100 - node.health) * 0.3   
        score += node.load * 0.2              
        if node.role == "Worker":
            score += 10                       

        candidates.append((nid, node, score))

    
    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates


def _ask_llm_shed_decision(network, timeout_rate, candidates):
    """
    Ask the LLM which nodes to shed given the congestion state.
    The LLM gets full cluster context and must return a JSON list of node IDs.
    Falls back to top-ranked candidates if LLM fails.
    """
    snapshot = get_cluster_snapshot(network)
    candidate_text = "\n".join([
        f"  {nid} ({node.role}): health={node.health:.0f}%, load={node.load:.0f}%, status={node.status}"
        for nid, node, _score in candidates
    ])

    total_nodes = len(network.nodes)
    max_to_shed = max(1, int(total_nodes * MAX_SHED_FRACTION)) - len(_shed_nodes)
    if max_to_shed <= 0:
        return []

    system_prompt = """You are an autonomous distributed systems capacity manager.
The cluster is CHOKING — timeout rate is critically high and commits are failing.
You must perform LOAD SHEDDING: gracefully remove non-essential nodes to reduce
inter-node communication overhead and restore the cluster's ability to reach quorum.

Respond ONLY with a valid JSON object — no markdown, no explanation:
{
  "shed_node_ids": ["N4", "N12"],
  "reason": "one sentence explaining the shedding strategy"
}

HARD CONSTRAINTS:
- NEVER shed Primary or Replica nodes — they are the consensus group.
- Only shed from the candidate list provided.
- Shed the minimum number needed to relieve congestion.
- Prefer shedding degraded/unhealthy nodes over healthy ones.
- Prefer shedding Workers over Gateways when health is similar."""

    prompt = f"""CONGESTION ALERT: timeout_rate = {timeout_rate*100:.1f}%

Cluster state:
{snapshot}

Eligible shed candidates (ranked by expendability):
{candidate_text}

Maximum nodes you may shed now: {max_to_shed}
Currently shed: {sorted(_shed_nodes) if _shed_nodes else 'none'}

Choose which nodes to shed to restore cluster health. Shed the minimum needed."""

    raw = ask_groq(prompt, system_prompt)
    if not raw:
        
        return [candidates[0][0]] if candidates else []

    try:
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        shed_ids = result.get("shed_node_ids", [])

        
        valid_ids = {nid for nid, _, _ in candidates}
        shed_ids = [nid for nid in shed_ids if nid in valid_ids]
        
        shed_ids = shed_ids[:max_to_shed]
        return shed_ids

    except Exception:
        
        return [candidates[0][0]] if candidates else []


def _execute_shed(network, node_id, current_time):
    """
    Gracefully shed a node: drain its load, mark it as failed,
    and partition it from the cluster.
    """
    node = network.nodes.get(node_id)
    if not node:
        return f"Node {node_id} not found"

    
    excess = node.load
    healthy_targets = [
        n for nid, n in network.nodes.items()
        if nid != node_id
        and n.status == "healthy"
        and nid not in _shed_nodes
    ]

    if healthy_targets and excess > 0:
        per_node = excess / len(healthy_targets)
        for n in healthy_targets:
            n.load = min(100, n.load + per_node)

    
    node.status = "failed"
    node.health = 0.0
    node.load = 0.0
    if hasattr(node, "queue_length"):
        node.queue_length = 0

    
    for other_id in network.nodes:
        if other_id != node_id:
            network.partition(node_id, other_id)

    with _shed_lock:
        _shed_nodes.add(node_id)

    return f"Shed {node_id} ({node.role}) — load redistributed to {len(healthy_targets)} nodes"


def _execute_readmit(network, node_id, current_time):
    """
    Re-admit a previously shed node: restart it and rejoin the cluster.
    """
    node = network.nodes.get(node_id)
    if not node:
        return f"Node {node_id} not found"

    
    if hasattr(network, "rejoin_node"):
        network.rejoin_node(
            node_id,
            preferred_role=node.role,
            degraded=False,
            health=90.0,
            load=10.0,  
        )
    else:
        node.status = "healthy"
        node.health = 90.0
        node.load = 10.0
        if hasattr(node, "queue_length"):
            node.queue_length = 0

    with _shed_lock:
        _shed_nodes.discard(node_id)

    return f"Re-admitted {node_id} — health=90%, load=10%"


def check_load_shedding(network, current_time):
    """
    Main load shedding controller. Call this every tick from main.py.

    Returns a list of (action_description, action_type, target) tuples
    for logging, matching the format of decide_and_recover output.

    Logic:
      1. If timeout_rate > SHED_TIMEOUT_THRESHOLD and not at max shed capacity:
         → Ask LLM which nodes to shed, then execute shedding
      2. If timeout_rate < READMIT_TIMEOUT_THRESHOLD and nodes are shed:
         → Re-admit shed nodes one at a time (gradual recovery)
      3. Otherwise: do nothing
    """
    global _shedding_active

    timeout_rate = _get_timeout_rate(network)
    actions_taken = []

    
    if timeout_rate > SHED_TIMEOUT_THRESHOLD:
        total_nodes = len(network.nodes)
        max_shed = max(1, int(total_nodes * MAX_SHED_FRACTION))

        if len(_shed_nodes) >= max_shed:
            if not _shedding_active:
                _shedding_active = True
                print(
                    f"[LOAD SHEDDING] Already at max shed capacity "
                    f"({len(_shed_nodes)}/{max_shed}), cannot shed more"
                )
            return actions_taken

        candidates = _rank_shed_candidates(network)
        if not candidates:
            print("[LOAD SHEDDING] No eligible candidates to shed")
            return actions_taken

        _shedding_active = True
        shed_ids = _ask_llm_shed_decision(network, timeout_rate, candidates)

        for node_id in shed_ids:
            result = _execute_shed(network, node_id, current_time)
            record_action(current_time, "load_shed", node_id, result, source="llm_shed")
            actions_taken.append((
                f"[LOAD SHEDDING] {result} (timeout_rate={timeout_rate*100:.1f}%)",
                "load_shed",
                node_id,
            ))
            pass  

    
    elif _shed_nodes and (timeout_rate < READMIT_TIMEOUT_THRESHOLD or
                          (timeout_rate < SHED_TIMEOUT_THRESHOLD and len(_shed_nodes) > 1)):
        
        with _shed_lock:
            readmit_id = next(iter(_shed_nodes))  

        result = _execute_readmit(network, readmit_id, current_time)
        record_action(current_time, "load_shed_readmit", readmit_id, result, source="llm_shed")
        actions_taken.append((
            f"[LOAD SHEDDING READMIT] {result} (timeout_rate={timeout_rate*100:.1f}%)",
            "load_shed_readmit",
            readmit_id,
        ))
        pass  

        
        if not _shed_nodes:
            _shedding_active = False
            pass  # logged by main.py

   
    elif timeout_rate <= SHED_TIMEOUT_THRESHOLD and not _shed_nodes:
        if _shedding_active:
            _shedding_active = False

    return actions_taken


def _build_few_shot_examples():
    """
    Few-shot examples for chain-of-thought reasoning.
    Each example shows the LLM HOW to reason about distributed system failures.
    """
    return """
=== EXAMPLE 1: Memory leak on a Replica with cascading risk ===
Failure: memory_leak on N3 (Replica), health=45%, 2 other nodes degraded
Chain of thought:
- N3 is a Replica at 45% health — it's still serving reads but degrading fast.
- 2 other nodes are already degraded, so isolating N3 would risk quorum (need majority of voters).
- Restarting N3 clears memory immediately and restores it to 90% health without losing quorum.
- Isolation would take 5+ ticks to recover vs restart which is immediate.
Decision: {"action": "restart_node", "target": "N3", "reason": "Restart clears memory leak immediately; isolation would risk quorum with 2 other degraded nodes", "urgency": "high", "confidence": 85, "diverges_from_rule": true}

=== EXAMPLE 2: Latency spike with high cluster load ===
Failure: latency_spike on network, avg cluster load=72%, N2 at 95% load
Chain of thought:
- Global latency spike means all nodes are affected, but N2 is critically overloaded.
- Rebalancing would spread N2's load evenly, but other nodes are already at 60-70%.
- Better to reroute specifically from N2 to the least-loaded nodes to prevent queue overflow.
- This preserves capacity on healthier nodes while relieving the bottleneck.
Decision: {"action": "reroute_traffic", "target": "N2", "reason": "Targeted reroute from overloaded N2 is better than rebalance when avg load is 72%", "urgency": "high", "confidence": 80, "diverges_from_rule": true}

=== EXAMPLE 3: Primary crash with healthy replicas ===
Failure: crash on N1 (Primary), 2 healthy replicas available, clean cluster
Chain of thought:
- Primary is down but 2 healthy replicas exist — standard failover scenario.
- The system will auto-elect a new primary, but we should ensure the crashed node restarts cleanly.
- Restarting N1 as a Replica is the safe default — it rejoins without disrupting the new primary.
Decision: {"action": "restart_node", "target": "N1", "reason": "Standard primary crash recovery; healthy replicas available for automatic failover", "urgency": "critical", "confidence": 92, "diverges_from_rule": false}
"""


def _multi_turn_reasoning(network, failure_event, initial_decision):
    """
    Multi-turn LLM interaction: after an initial decision, the LLM re-evaluates
    by examining whether its decision could cause secondary issues.
    Returns a refined decision if the LLM changes its mind, otherwise the original.
    """
    action = initial_decision.get("action", "monitor_only")
    target = initial_decision.get("target", failure_event.target)
    reason = initial_decision.get("reason", "")
    confidence = initial_decision.get("confidence", 0)

    
    if action in ("monitor_only", "skipped", "none", "parse_failed"):
        return initial_decision
    if confidence > 90:
        return initial_decision

    snapshot = get_cluster_snapshot(network)
    context = _build_situational_context(network, failure_event)

    consensus_nodes = [n for n in network.nodes.values() if n.role in ("Primary", "Replica")]
    healthy_consensus = [n for n in consensus_nodes if n.status != "failed"]

    verification_prompt = f"""You previously decided: {action} on {target}
Reason: {reason}
Confidence: {confidence}%

Current cluster state:
{snapshot}

Situation:
{context}

Now critically evaluate your decision:
1. If you chose isolate_node: will this break quorum? (Need majority of {len(consensus_nodes)} consensus nodes healthy, currently {len(healthy_consensus)} healthy)
2. If you chose restart_node: is the node's health too low (<10%) for restart to succeed?
3. Could your action trigger a cascade failure on other nodes?
4. Is there a BETTER action you missed? Consider the trade-offs.
5. Are there concurrent failures that change the priority?

If your original decision is still best, return it unchanged.
If you want to change it, return the new decision.

Respond ONLY with a valid JSON object:
{{
  "action": one of [reassign_role, restart_node, reroute_traffic, isolate_node, rebalance_load, monitor_only],
  "target": exact node ID,
  "reason": one sentence explaining your FINAL reasoning after re-evaluation,
  "urgency": one of [critical, high, medium, low],
  "confidence": integer 0-100,
  "diverges_from_rule": true or false,
  "reasoning_changed": true if you changed your mind, false if you kept original
}}"""

    system_prompt = """You are a distributed systems expert doing a SECOND PASS review of a recovery decision.
Be critical. Check for quorum safety, cascade risks, and whether a different action would be more effective.
Respond ONLY with valid JSON — no markdown, no explanation."""

    raw = ask_groq(verification_prompt, system_prompt)
    if not raw:
        return initial_decision

    refined = _parse_llm_json(raw, failure_event.target)
    refined_action = refined.get("action", action)

   
    if refined.get("reasoning_changed", False) or refined_action != action:
        print(
            f"[MULTI-TURN] LLM revised: {action} → {refined_action} on "
            f"{refined.get('target', target)} (confidence: {confidence}% → {refined.get('confidence', 0)}%)"
        )

    return refined


def decide_only(network, failure_event, current_time=0):
    key = f"{failure_event.target}-{failure_event.failure_type}"
    if key in _last_action_time:
        if current_time - _last_action_time[key] < COOLDOWN_TICKS:
            remaining = COOLDOWN_TICKS - (current_time - _last_action_time[key])
            return {
                "action": "skipped", "target": failure_event.target,
                "reason": f"Cooldown active ({remaining:.0f} ticks remaining)",
                "urgency": "low", "confidence": 100, "diverges_from_rule": False,
            }

    
    if failure_event.target in _shed_nodes:
        return {
            "action": "monitor_only", "target": failure_event.target,
            "reason": f"Node {failure_event.target} is currently shed for load shedding — skipping recovery",
            "urgency": "low", "confidence": 100, "diverges_from_rule": False,
        }

    snapshot = get_cluster_snapshot(network)
    history  = get_recent_history()
    context  = _build_situational_context(network, failure_event)
    few_shot = _build_few_shot_examples()
    node = network.nodes.get(failure_event.target)
    node_role = node.role if node else None
    learning_context = decision_memory.get_relevant_context(
        failure_event.failure_type, node_role=node_role
    )

    system_prompt = """You are an autonomous distributed systems recovery engine with deep expertise
in consensus protocols, quorum management, and failure recovery.

You must analyze the cluster state using chain-of-thought reasoning:
1. First, assess the SEVERITY: How many nodes are affected? Is quorum at risk?
2. Then, consider TRADE-OFFS: Each action has costs. Isolation is safe but slow. Restart is fast but risky if health is very low.
3. Then, check for CASCADES: Could your action make things worse? Will other nodes be affected?
4. Finally, choose the OPTIMAL action that maximizes cluster stability.

Respond ONLY with a valid JSON object — no markdown, no explanation, no code fences.

Required fields:
{
  "action": one of [reassign_role, restart_node, reroute_traffic, isolate_node, rebalance_load, monitor_only],
  "target": exact node ID (never "network"),
  "chain_of_thought": a 2-3 sentence reasoning trace showing HOW you reached your decision,
  "reason": one concrete sentence summarizing WHY this action is best,
  "urgency": one of [critical, high, medium, low],
  "confidence": integer 0-100,
  "diverges_from_rule": true if your action differs from the default rule action shown in the context
}

Available actions and what they do:
- restart_node: Restarts the node, clearing memory and restoring health to ~90%. Node stays in cluster.
- isolate_node: Removes the node from cluster entirely. Goes to failed state, must be restarted later.
- rebalance_load: Equalizes load across all healthy nodes.
- reroute_traffic: Moves 60% of a specific node's load to other healthy nodes.
- reassign_role: Changes a healthy node's role (e.g., Replica becomes Primary).
- monitor_only: Take no action, continue observing.

Constraints:
- Never restart a node that is already healthy with health > 80%.
- Never reassign_role to an unhealthy or failed node.
- Never isolate if it would leave fewer than 2 healthy nodes.
- If LOAD SHEDDING MODE is active, do NOT attempt to recover shed nodes.
- During load shedding, focus only on keeping consensus nodes (Primary/Replica) healthy.

IMPORTANT: Think step by step. Show your reasoning in chain_of_thought."""

    learning_block = f"\n\n{learning_context}" if learning_context else ""

    prompt = f"""=== FEW-SHOT EXAMPLES (learn the reasoning pattern) ===
{few_shot}
{learning_block}

=== CURRENT SITUATION ===
Cluster state:
{snapshot}

Context:
{context}

Recent actions:
{history}

Failure:
Type: {failure_event.failure_type}
Affected: {failure_event.target}
Time: t={failure_event.time:.1f}

Think step by step: What is the severity? What are the trade-offs? Could your action cascade?
Then choose the best recovery action."""

    raw      = ask_groq(prompt, system_prompt)
    decision = _parse_llm_json(raw, failure_event.target)

    action = decision.get("action", "monitor_only")
    target = decision.get("target", failure_event.target)
    target = _normalize_target(target, network, failure_event.target)

   
    cot = decision.get("chain_of_thought", "")
    
    _ = cot  

    
    rule_default = RULE_DEFAULTS.get(failure_event.failure_type, "monitor_only")
    if action != rule_default:
        decision["diverges_from_rule"] = True

    decision["action"]             = action
    decision["target"]             = target
    decision["reason"]             = decision.get("reason", "No reason given")
    decision["urgency"]            = decision.get("urgency", "medium")
    decision["confidence"]         = decision.get("confidence", 0)
    decision["diverges_from_rule"] = decision.get("diverges_from_rule", False)

    
    concurrent_failures = sum(
        1 for n in network.nodes.values() if n.status in ("failed", "degraded")
    )
    if concurrent_failures >= 2 or decision.get("confidence", 0) < 75:
        decision = _multi_turn_reasoning(network, failure_event, decision)

    return decision


def decide_and_recover(network, failure_event, current_time=0):
    global _llm_overrides, _rule_echoes

    cache_key = _make_cache_key(failure_event, network)
    key       = f"{failure_event.target}-{failure_event.failure_type}"

    if key in _last_action_time:
        if current_time - _last_action_time[key] < COOLDOWN_TICKS:
            remaining = COOLDOWN_TICKS - (current_time - _last_action_time[key])
            return (f"Cooldown active ({remaining:.0f} ticks remaining)", "skipped", failure_event.target)

    
    if failure_event.target in _shed_nodes:
        return (
            f"[SHED] Node {failure_event.target} is shed for load shedding — skipping recovery",
            "monitor_only",
            failure_event.target,
        )

    if cache_key in _decision_cache:
        cached = _decision_cache[cache_key]
        action = cached["action"]
        target = cached.get("target", failure_event.target)
        valid, _ = validate_action(action, target, network, failure_event.failure_type)
        if valid:
            result = execute_action(network, action, target, failure_event, current_time)
            record_action(current_time, action, target, result, source="llm_cached")
            _last_action_time[key] = current_time
            return f"[CACHED] Action: {action} on {target} -> {result}", action, target

    decision = decide_only(network, failure_event, current_time)

    action    = decision.get("action", "monitor_only")
    target    = decision.get("target", failure_event.target)
    reason    = decision.get("reason", "No reason given")
    urgency   = decision.get("urgency", "medium")
    confidence = decision.get("confidence", 0)
    diverges  = decision.get("diverges_from_rule", False)

    if diverges:
        _llm_overrides += 1
    else:
        _rule_echoes += 1

    if action in ["none", "parse_failed", "skipped"]:
        return (
            f"[{urgency.upper()}] [{confidence}% confident] Action: {action} on {target} -- {reason}",
            action, target
        )

    valid, validation_msg = validate_action(action, target, network, failure_event.failure_type)
    if not valid:
        pass  # validation logged in decision summary
        action = "monitor_only"
        reason = f"Blocked: {validation_msg}"

    result = execute_action(network, action, target, failure_event, current_time)
    record_action(current_time, action, target, result, source="llm")
    _last_action_time[key] = current_time

    if len(_decision_cache) >= CACHE_MAX_SIZE:
        _decision_cache.pop(next(iter(_decision_cache)))
    _decision_cache[cache_key] = {"action": action, "target": target}

    diverge_tag = " [LLM OVERRIDE]" if diverges else ""
    summary = (
        f"[{urgency.upper()}] [{confidence}% confident]{diverge_tag} "
        f"Action: {action} on {target} -- {reason} -> Result: {result}"
    )
    return summary, action, target


def _schedule_post_isolation_restart(network, node_id, current_time, env=None):
    """
    IMPROVEMENT: After isolating a node, schedule a restart after a short
    recovery window so the node eventually comes back healthy instead of
    staying permanently failed.
    The actual restart is executed via the feedback / follow-up loop — we
    store a pending restart here and main.py polls _pending_restarts.
    """
    with _restart_lock:
        if node_id not in _pending_restarts:
            _pending_restarts[node_id] = current_time + 5.0   


def get_pending_restarts(current_time):
    """
    Return list of node IDs whose post-isolation restart is now due.
    Called from main.py decision loop.
    """
    due = []
    with _restart_lock:
        for node_id, restart_at in list(_pending_restarts.items()):
            if current_time >= restart_at:
                due.append(node_id)
                del _pending_restarts[node_id]
    return due


def execute_action(network, action, target, failure_event, current_time=0):
    """Execute an action. Callers MUST call record_action() separately."""
    if target == "network":
        healthy = [n for n in network.nodes.values() if n.status == "healthy"]
        if healthy:
            target = max(healthy, key=lambda n: n.load).id

    node = network.nodes.get(target)

    if action == "reassign_role":
        if node and node.status == "healthy":
            failed_node = network.nodes.get(failure_event.target)
            if failed_node:
                old_role = node.role
                node.role = failed_node.role
                node.health = min(100, node.health + 20)

                if node.role == "Primary" and hasattr(network, "set_primary"):
                    network.set_primary(node.id, count_election=True)

                return f"Reassigned {node.id} from {old_role} to {node.role}"
            return f"Original failed node {failure_event.target} not found"
        return "Target node is not healthy or not found"

    elif action == "restart_node":
        if (
            hasattr(failure_event, "failure_type")
            and failure_event.failure_type in (
                "memory_leak", "memory_leak_early", "predicted", "load_shed_readmit",
            )
            and hasattr(failure_event, "resolve")
        ):
            failure_event.resolve(current_time)
            parent = getattr(failure_event, "_parent_event", None)
            if parent and not parent.resolved:
                parent.resolve(current_time)

        if not node:
            return f"Node {target} not found"

        if hasattr(network, "rejoin_node"):
            
            preferred_role = node.role
            if node.role == "Primary":
                current_primary = getattr(network, "current_primary", None)
                if current_primary is not None and current_primary != node.id:
                    preferred_role = "Replica"

            network.rejoin_node(
                target,
                preferred_role=preferred_role,
                degraded=False,
                health=90.0,
                load=20.0,
            )
        else:
            node.status = "healthy"
            node.health = 90.0
            node.load = 20.0
            if hasattr(node, "queue_length"):
                node.queue_length = 0

        return f"Restarted {target} -- memory cleared, queue drained, health restored to 90%"

    elif action == "isolate_node":
        if not node:
            return f"Node {target} not found"

        if (
            hasattr(failure_event, "failure_type")
            and failure_event.failure_type in ("memory_leak", "memory_leak_early", "predicted")
            and hasattr(failure_event, "resolve")
        ):
            failure_event.resolve(current_time)
            parent = getattr(failure_event, "_parent_event", None)
            if parent and not parent.resolved:
                parent.resolve(current_time)

        node.status = "failed"
        node.health = 0.0
        node.load = 0.0
        if hasattr(node, "queue_length"):
            node.queue_length = 0

        for other_id in network.nodes:
            if other_id != target:
                network.partition(target, other_id)

        if getattr(network, "current_primary", None) == target and hasattr(network, "elect_new_primary"):
            network.elect_new_primary()

        _schedule_post_isolation_restart(network, target, current_time)
        return f"Isolated {target} from all peers — restart scheduled in 5 ticks"

    elif action == "reroute_traffic":
        if node:
            excess = node.load * 0.5
            node.load = node.load * 0.5
            if hasattr(node, "queue_length"):
                node.queue_length = max(0, int(node.queue_length * 0.6))

            healthy_nodes = [
                n for nid, n in network.nodes.items()
                if nid != target
                and n.status == "healthy"
                and nid not in _shed_nodes  
            ]
            if healthy_nodes:
                per_node = excess / len(healthy_nodes)
                for n in healthy_nodes:
                    n.load = min(100, n.load + per_node)

            return f"Rerouted {excess:.0f}% load from {target} to {len(healthy_nodes)} nodes"
        return f"Node {target} not found"

    elif action == "rebalance_load":
        healthy = [
            (nid, n) for nid, n in network.nodes.items()
            if n.status == "healthy" and nid not in _shed_nodes  
        ]
        if healthy:
            total = sum(n.load for _, n in healthy)
            avg = total / len(healthy)
            for _, n in healthy:
                n.load = avg
            return f"Rebalanced load to {avg:.0f}% across {len(healthy)} nodes"
        return "No healthy nodes to rebalance"

    elif action == "monitor_only":
        return "Monitoring -- no action taken"

    elif action == "promote_replica":
        if hasattr(network, "elect_new_primary"):
            new_primary = network.elect_new_primary()
            if new_primary:
                return f"Promoted {new_primary} to Primary"
            return "No eligible replica available for promotion"
        return "Promotion not supported by network"

    return f"Unknown action: {action}"


HEALTHY = "healthy"


def analyze_simulation(network, failure_history, action_history=None, scorer_summary=None):
    """
    Grounded post-incident analysis.
    v2: Now includes decision scoring data for LLM vs Rule comparison.
    v3: Includes load shedding events in the analysis.
    """
    snapshot = get_cluster_snapshot(network)

    failures_text = "\n".join([
        f"- t={f.time:.1f}: {f.failure_type} on {f.target} "
        f"({'resolved at t=' + str(round(f.resolved_at, 1)) if f.resolved and f.resolved_at else 'STILL ACTIVE'})"
        for f in failure_history
    ])

    history_to_use = action_history if action_history is not None else _action_history
    if history_to_use:
        actions_text = "\n".join([
            f"- t={h['time']}: [{h.get('source','llm').upper()}] {h['action']} on {h['target']} -> {h['result']}"
            for h in history_to_use
        ])
    else:
        actions_text = "No recovery actions were recorded."

    # Count load shedding events
    shed_actions = [h for h in history_to_use if h.get("action") == "load_shed"]
    readmit_actions = [h for h in history_to_use if h.get("action") == "load_shed_readmit"]

    total    = len(failure_history)
    resolved = sum(1 for f in failure_history if f.resolved)
    active   = total - resolved

    mttr_values = [f.resolved_at - f.time for f in failure_history if f.resolved and f.resolved_at]
    avg_mttr    = round(sum(mttr_values) / len(mttr_values), 1) if mttr_values else None

    metrics_text = f"Failures: {total} total, {resolved} resolved, {active} still active"
    if avg_mttr:
        metrics_text += f"\nAverage MTTR: {avg_mttr} simulation ticks"

    net_stats    = network.get_stats()
    metrics_text += f"\nMessage delivery rate: {net_stats['success_rate']}"
    metrics_text += f"\nLLM novel overrides recorded: {_llm_overrides}"
    if shed_actions:
        metrics_text += f"\nLoad shedding events: {len(shed_actions)} sheds, {len(readmit_actions)} re-admissions"
        shed_targets = [h['target'] for h in shed_actions]
        metrics_text += f"\nNodes shed: {shed_targets}"

   
    proactive_actions = [h for h in history_to_use if h.get("source") == "llm_proactive"]
    if proactive_actions:
        metrics_text += f"\nProactive optimizations: {len(proactive_actions)}"

   
    scoring_section = ""
    if scorer_summary:
        scoring_section = "\n\n=== DECISION SCORING DATA ===\n"
        for source, data in scorer_summary.items():
            scoring_section += (
                f"\n{source.upper()} decisions: {data.get('count', 0)} scored, "
                f"avg_score={data.get('avg_total_score', 'N/A')}, "
                f"avg_mttr={data.get('avg_mttr', 'N/A')} ticks, "
                f"resolved={data.get('resolved_pct', 'N/A')}%, "
                f"diverged_from_rules={data.get('diverged_count', 0)}"
            )

    
    node_states = "\n".join([
        f"  {nid} ({n.role}): status={n.status}, health={n.health:.0f}%, load={n.load:.0f}%"
        + (" [SHED]" if nid in _shed_nodes else "")
        for nid, n in network.nodes.items()
    ])

    system_prompt = """You are a distributed systems reliability engineer writing a post-incident report.
STRICT RULE: Only reference nodes, events, timestamps, and actions from the data sections below.
Do NOT invent events. Do NOT reference a timestamp unless it appears in the logs.
Write complete sentences. Do not cut off mid-sentence.
If load shedding events occurred, evaluate their effectiveness at reducing congestion.
If proactive optimizations occurred, evaluate whether they prevented failures or improved performance."""

    prompt = f"""Write a full resilience report based ONLY on the data below.

=== FINAL CLUSTER STATE ===
{node_states}

=== FAILURE EVENTS ===
{failures_text}
{metrics_text}

=== RECOVERY ACTIONS EXECUTED ===
{actions_text}
{scoring_section}

=== YOUR TASK ===
1. Resilience score out of 10 with clear justification referencing the data above.
2. 3 things that worked well — cite specific events, timestamps, and decision sources (RULE vs LLM).
3. 3 concrete improvement suggestions tied to specific weaknesses.
4. If scoring data is available: compare LLM vs Rule decision quality with specific numbers.
5. If load shedding occurred: evaluate whether it improved commit rates and timeout rates.
6. If proactive optimizations occurred: evaluate whether they prevented degradation.

Write complete sentences. Do not truncate."""

    return ask_groq(prompt, system_prompt, max_tokens=1400) or "Analysis unavailable"


def optimize_cluster(network):
    """
    Proactive LLM optimization — analyzes cluster health and suggests
    preemptive actions BEFORE failures occur. This is a key differentiator
    from pure rule-based systems: the LLM can identify emerging patterns
    and optimize proactively.
    """
    snapshot = get_cluster_snapshot(network)
    stats = network.get_stats() if hasattr(network, "get_stats") else {}

    
    loads = []
    health_risks = []
    for nid, node in network.nodes.items():
        if node.status != "failed" and nid not in _shed_nodes:
            loads.append(node.load)
            if node.health < 70:
                health_risks.append(f"{nid} ({node.role}): health={node.health:.0f}%")

    load_variance = 0
    if loads:
        avg_load = sum(loads) / len(loads)
        load_variance = sum((l - avg_load) ** 2 for l in loads) / len(loads)

    system_prompt = """You are a proactive distributed systems optimizer. Your job is to
identify issues BEFORE they become failures and suggest preventive actions.

Respond ONLY with valid JSON — no markdown:
{
  "actions": [
    {
      "action": one of [rebalance_load, reroute_traffic, monitor_only],
      "target": node ID or "cluster",
      "reason": one sentence
    }
  ],
  "risk_assessment": one sentence about overall cluster health,
  "optimization_summary": 2-3 bullet points about what you'd improve
}

Focus on:
- Load imbalance (variance > 200 means uneven distribution)
- Nodes trending toward degradation (health < 70%)
- Timeout rate trends (rising timeouts predict future failures)
- Queue buildup on specific nodes"""

    prompt = f"""Cluster state:
{snapshot}

Performance metrics:
- Timeout rate: {stats.get('timeout_rate', '0%')}
- Commit rate: {stats.get('commit_rate', '0%')}
- Stale read rate: {stats.get('stale_read_rate', '0%')}
- Load variance: {load_variance:.1f} (>200 = imbalanced)
- Quorum status: {'available' if stats.get('quorum_available_ticks', 0) > stats.get('quorum_unavailable_ticks', 0) else 'at risk'}

Health risks:
{chr(10).join(health_risks) if health_risks else 'No immediate health risks'}

Analyze the cluster and suggest proactive optimizations to prevent future failures."""

    raw = ask_groq(prompt, system_prompt)
    if not raw:
        return "No optimization available"

    
    try:
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)

        
        actions = result.get("actions", [])
        executed = []
        for act in actions:
            action = act.get("action", "monitor_only")
            target = act.get("target", "cluster")

            if action == "rebalance_load":
                healthy = [
                    (nid, n) for nid, n in network.nodes.items()
                    if n.status == "healthy" and nid not in _shed_nodes
                ]
                if healthy and load_variance > 200:
                    total = sum(n.load for _, n in healthy)
                    avg = total / len(healthy)
                    for _, n in healthy:
                        n.load = avg
                    executed.append(f"Rebalanced load to {avg:.0f}% across {len(healthy)} nodes")

            elif action == "reroute_traffic" and target in network.nodes:
                node = network.nodes[target]
                if node.load > 70:
                    excess = node.load * 0.3
                    node.load -= excess
                    targets = [
                        n for nid, n in network.nodes.items()
                        if nid != target and n.status == "healthy" and nid not in _shed_nodes
                    ]
                    if targets:
                        per = excess / len(targets)
                        for n in targets:
                            n.load = min(100, n.load + per)
                        executed.append(f"Proactively rerouted {excess:.0f}% from {target}")

        summary = result.get("optimization_summary", "No optimization available")
        risk = result.get("risk_assessment", "")

        parts = []
        if risk:
            parts.append(f"Risk: {risk}")
        if executed:
            parts.append(f"Actions taken: {'; '.join(executed)}")
        if isinstance(summary, list):
            parts.append("\n".join(f"- {s}" for s in summary))
        elif isinstance(summary, str):
            parts.append(summary)

        return "\n".join(parts) if parts else "No optimization needed"

    except Exception:
        return raw or "No optimization available"


def reset_llm_state():
    global _llm_overrides, _rule_echoes, _shedding_active
    _last_action_time.clear()
    _action_history.clear()
    _decision_cache.clear()
    _pending_restarts.clear()
    _shed_nodes.clear()
    _llm_overrides = 0
    _rule_echoes   = 0
    _shedding_active = False


def is_obvious_case(failure_event, network):
    """
    IMPROVEMENT: memory_leak and memory_leak_early always go to LLM so it
    v2: Now uses confidence-based tier routing from RuleBasedRecovery.
    Tier 1 (confidence >= 80): rules handle autonomously.
    Tier 2 (40-79): LLM decides, rules validate.
    Tier 3 (< 40): LLM decides with emergency authority.
    """
    from rule_based import RuleBasedRecovery
    _router = RuleBasedRecovery()
    confidence, tier, reason = _router.assess_confidence(network, failure_event)

    
    tier_label = {1: "TIER-1/RULE", 2: "TIER-2/LLM+VALIDATE", 3: "TIER-3/LLM-EMERGENCY"}
    print(
        f"[ROUTER] {failure_event.failure_type} on {failure_event.target}: "
        f"confidence={confidence}% → {tier_label.get(tier, '?')} ({reason})"
    )

    
    return tier == 1


def decide_batch(network, failures, current_time=0):
    """Coordinated multi-failure decisions with chain-of-thought and triage reasoning."""
    if len(failures) == 1:
        return [decide_only(network, failures[0], current_time)]

    snapshot = get_cluster_snapshot(network)
    context = _build_situational_context(network, failures[0])
    failures_text = "\n".join([
        f"{i+1}. type={f.failure_type}, node={f.target}, time={f.time:.1f}"
        for i, f in enumerate(failures)
    ])

    
    failure_details = []
    for f in failures:
        node = network.nodes.get(f.target)
        if node:
            failure_details.append(
                f"  {f.target} ({node.role}): health={node.health:.0f}%, "
                f"load={node.load:.0f}%, status={node.status}"
            )
    details_text = "\n".join(failure_details)

    system_prompt = """You are an autonomous distributed systems recovery engine handling MULTIPLE
simultaneous failures. This requires TRIAGE — you must prioritize which failures to address first.

Use this triage framework:
1. QUORUM SAFETY FIRST: If a consensus node (Primary/Replica) is affected, address it before Workers/Gateways.
2. PREVENT CASCADES: If one failure could cause others, address the root cause first.
3. MINIMIZE ISOLATION: Don't isolate multiple nodes simultaneously — that kills quorum.
4. COORDINATE ACTIONS: Actions on one node affect others (e.g., rebalance changes all loads).

For each failure, show your reasoning in chain_of_thought.

Respond ONLY with a valid JSON array — no markdown, no explanation.

Each element:
{
  "action": one of [reassign_role, restart_node, reroute_traffic, isolate_node, rebalance_load, monitor_only],
  "target": node ID (never "network"),
  "chain_of_thought": 1-2 sentence reasoning trace for THIS specific failure in context of the others,
  "reason": one concrete sentence,
  "urgency": one of [critical, high, medium, low],
  "confidence": integer 0-100,
  "diverges_from_rule": true or false,
  "triage_priority": integer 1-N (1 = handle first)
}

CONSTRAINTS:
- Don't isolate multiple nodes simultaneously — that kills quorum.
- Keep at least 1 Primary and 1 Replica healthy at all times.
- When multiple failures overlap, prioritize cluster stability over individual node recovery.
- If LOAD SHEDDING is active, do NOT attempt to recover shed nodes."""

    
    batch_learning = []
    for f in failures:
        f_node = network.nodes.get(f.target)
        f_role = f_node.role if f_node else None
        ctx = decision_memory.get_relevant_context(f.failure_type, node_role=f_role, limit=2)
        if ctx:
            batch_learning.append(ctx)
    learning_block = "\n".join(batch_learning) if batch_learning else ""

    prompt = f"""=== BATCH TRIAGE EXAMPLE ===
Simultaneous failures: crash on N1 (Primary) + memory_leak on N4 (Worker, health=35%)
Triage reasoning:
- N1 is Primary — quorum is at risk. This is priority 1.
- N4 is a Worker with memory leak — important but not consensus-critical. Priority 2.
- Restart N1 immediately; restart N4 to clear memory (health still >20% so restart is viable).
- Do NOT isolate N4 while N1 is down — that would lose too many nodes.
{f'''
{learning_block}''' if learning_block else ''}

=== CURRENT SITUATION ===
Cluster state:
{snapshot}

Context:
{context}

Affected nodes:
{details_text}

Simultaneous failures (TRIAGE NEEDED):
{failures_text}

Think about priority order. Which failure is most dangerous to cluster stability?
Respond with a JSON array, one decision per failure."""

    raw = ask_groq(prompt, system_prompt)
    if not raw:
        return [decide_only(network, f, current_time) for f in failures]

    try:
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        decisions = json.loads(raw)
    except Exception:
        return [decide_only(network, f, current_time) for f in failures]

    normalized = []
    for failure, dec in zip(failures, decisions):
        action = dec.get("action", "monitor_only")
        target = _normalize_target(dec.get("target", failure.target), network, failure.target)
        normalized.append({
            "action":             action,
            "target":             target,
            "reason":             dec.get("reason", "No reason given"),
            "urgency":            dec.get("urgency", "medium"),
            "confidence":         dec.get("confidence", 0),
            "diverges_from_rule": dec.get("diverges_from_rule", False),
        })

    while len(normalized) < len(failures):
        failure = failures[len(normalized)]
        normalized.append({
            "action": "monitor_only", "target": failure.target,
            "reason": "Missing batch decision", "urgency": "low",
            "confidence": 0, "diverges_from_rule": False,
        })

    return normalized