from llm_agent import ask_groq, get_cluster_snapshot


_pending_checks = []

def reset_feedback_state():
    """Clear feedback state between simulation runs."""
    global _pending_checks
    _pending_checks = []

def schedule_feedback_check(node_id, action, time_executed, network):
    """Schedule a feedback check 5 ticks after an action."""
    global _pending_checks

    
    if action in ["skipped", "monitor_only", "parse_failed", "none", None]:
        return

    
    for check in _pending_checks:
        if (
            not check["checked"]
            and check["node_id"] == node_id
            and check["action"] == action
        ):
            return

    _pending_checks.append({
        "node_id": node_id,
        "action": action,
        "time_executed": time_executed,
        "network": network,
        "checked": False
    })

def run_feedback_checks(current_time, network):
    """
    Run pending feedback checks.
    Called every tick from the main loop.
    Returns list of follow-up actions needed.
    """
    global _pending_checks

    follow_ups = []

    for check in _pending_checks:
        if check["checked"]:
            continue

        
        if current_time - check["time_executed"] < 5:
            continue

        check["checked"] = True
        node_id = check["node_id"]
        action = check["action"]
        node = network.nodes.get(node_id)

        if not node:
            continue

        
        success, reason = evaluate_action_result(node, action)

        if not success:
            print(
                f"[FEEDBACK] Action '{action}' on {node_id} "
                f"did not work — {reason}"
            )
            follow_up = get_follow_up_action(network, node_id, action, reason)
            if follow_up and follow_up.get("action") not in ["monitor_only", "none", "parse_failed"]:
                follow_ups.append(follow_up)
        else:
            print(
                f"[FEEDBACK] Action '{action}' on {node_id} "
                f"succeeded — {reason}"
            )

   
    _pending_checks = [c for c in _pending_checks if not c["checked"]]

    return follow_ups

def evaluate_action_result(node, action):
    """Check if the action had the desired effect."""

    if action == "promote_replica":
        if node.role == "Primary" and node.status == "healthy":
            return True, "Node successfully promoted to Primary"
        return False, f"Node role={node.role}, status={node.status}"

    elif action == "restart_node":
        if node.status == "healthy" and node.health >= 70:
            return True, f"Node healthy at {node.health:.0f}%"
        return False, f"Node still unhealthy — health={node.health:.0f}%"

    elif action == "isolate_node":
        if node.status in ["failed", "isolated"]:
            return True, f"Node isolated with status={node.status}"
        return False, f"Node not isolated — status={node.status}"

    elif action == "rebalance_load":
        if node.load <= 60:
            return True, f"Load balanced at {node.load:.0f}%"
        return False, f"Load still high at {node.load:.0f}%"

    elif action == "reroute_traffic":
        if node.load <= 50:
            return True, f"Traffic rerouted — load={node.load:.0f}%"
        return False, f"Load still high at {node.load:.0f}%"

    return True, "Action completed"

def get_follow_up_action(network, node_id, failed_action, reason):
    """Ask LLM what to do when an action didn't work."""
    snapshot = get_cluster_snapshot(network)

    prompt = f"""A recovery action failed and needs a follow-up.

Cluster state:
{snapshot}

Failed action: {failed_action} on {node_id}
Reason it failed: {reason}

What is the best follow-up recovery action?
Respond with ONLY raw JSON:
{{
  "action": one of [promote_replica, restart_node, reroute_traffic, isolate_node, rebalance_load, monitor_only],
  "target": node ID,
  "reason": one sentence
}}"""

    raw = ask_groq(prompt)
    if not raw:
        return None

    try:
        import json
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        decision = json.loads(raw)
        return {
            "node_id": decision.get("target", node_id),
            "action": decision.get("action", "monitor_only"),
            "reason": decision.get("reason", "Follow-up action")
        }
    except Exception:
        return None
