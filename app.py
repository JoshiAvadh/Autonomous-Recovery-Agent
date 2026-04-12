import threading
import time
import simpy
from flask import Flask, render_template, jsonify, request
from network import build_cluster
from failures import FaultInjector
from llm_agent import (
    decide_and_recover,
    optimize_cluster,
    analyze_simulation,
    ask_groq,
    get_cluster_snapshot,
    execute_action,
    reset_llm_state,
    record_action,
)
from scorer import DecisionScorer
from rule_based import RuleBasedRecovery
from predictor import predictor
from feedback import (
    schedule_feedback_check,
    run_feedback_checks,
    reset_feedback_state,
)
from cluster_config import generate_cluster, generate_failures
from config import ENABLE_FEEDBACK_FOLLOWUPS, ENABLE_OPTIMIZATION, ENABLE_PREDICTIVE_ACTIONS, OPTIMIZE_EVERY, SIMULATION_TIME
import json

app = Flask(__name__)

state = {
    "nodes": [],
    "stats": {},
    "events": [],
    "llm_decisions": [],
    "predictions": [],
    "score_history": [],  
    "sim_time": 0,
    "running": False,
    "complete": False,
    "cluster_size": 5,
    "final_success_rate": None,
    "final_delivered": 0,
    "final_dropped": 0,
}

_predictive_actions_taken = set()
_sim_thread = None


def add_event(msg, level="info"):
    state["events"].insert(0, {
        "msg": msg,
        "level": level,
        "time": round(state["sim_time"], 1)
    })
    state["events"] = state["events"][:40]

def update_nodes(network):
    state["nodes"] = [
        {
            "id": node.id,
            "role": node.role,
            "status": node.status,
            "health": node.health,
            "load": node.load,
        }
        for node in network.nodes.values()
    ]
    state["stats"] = network.get_stats()

def handle_predictive_alert(alert, network, current_time):
    node_id = alert["node_id"]
    if node_id in _predictive_actions_taken:
        return
    _predictive_actions_taken.add(node_id)

    snapshot = get_cluster_snapshot(network)
    node = network.nodes.get(node_id)
    if not node:
        return

    prompt = f"""A node is predicted to fail soon and needs PROACTIVE action NOW.

Cluster state:
{snapshot}

Prediction:
Node: {node_id} ({node.role})
Current health: {alert['health']:.0f}%
Predicted failure in: {alert['ticks']} ticks
Confidence: {alert['confidence']}%

What proactive action should be taken RIGHT NOW?
Respond with ONLY raw JSON:
{{
  "action": one of [reassign_role, restart_node, reroute_traffic, isolate_node, rebalance_load, monitor_only],
  "target": node ID,
  "reason": one sentence,
  "urgency": one of [critical, high, medium, low]
}}"""

    raw = ask_groq(prompt)
    if not raw:
        return

    try:
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        decision = json.loads(raw)
    except Exception:
        return

    action = decision.get("action", "monitor_only")
    target = decision.get("target", node_id)
    reason = decision.get("reason", "Proactive action")
    urgency = decision.get("urgency", "high")

    class MockFailure:
        def __init__(self, target):
            self.target = target
            self.failure_type = "predicted"

    result = execute_action(network, action, target, MockFailure(node_id))
    summary = (
        f"[PROACTIVE/{urgency.upper()}] Action: {action} on {target} "
        f"— {reason} → Result: {result}"
    )

    state["llm_decisions"].insert(0, {
        "time": round(current_time, 1),
        "failure": f"predicted_failure on {node_id}",
        "decision": summary,
        "action": action,
    })
    state["llm_decisions"] = state["llm_decisions"][:15]
    add_event(f"PROACTIVE: {action} on {node_id} — {reason}", "proactive")

_app_scorer = None
_app_rules = None
_app_pending_scores = {}

def llm_decision_loop(env, network, injector):
    global _app_scorer, _app_rules, _app_pending_scores
    _app_scorer = DecisionScorer()
    _app_rules = RuleBasedRecovery()
    _app_pending_scores = {}
    seen_failures = set()
    next_optimize = OPTIMIZE_EVERY

    while True:
        yield env.timeout(1)
        state["sim_time"] = env.now

        
        for event in injector.history:
            event_id = f"{event.failure_type}-{event.target}-{event.time:.1f}"
            if event_id in seen_failures:
                continue

            seen_failures.add(event_id)
            add_event(f"{event.failure_type.upper()} on {event.target}", "error")
            update_nodes(network)

            decision, action, action_target = decide_and_recover(network, event, env.now)
            state["llm_decisions"].insert(0, {
                "time": round(env.now, 1),
                "failure": f"{event.failure_type} on {event.target}",
                "decision": decision,
                "action": action,
            })
            state["llm_decisions"] = state["llm_decisions"][:15]

            add_event(f"Action: {action} on {action_target}", "llm")
            if "→ Result:" in decision:
                add_event(decision.split("→ Result:")[-1].strip(), "success")

            if action not in ["skipped", "monitor_only", "parse_failed", "none", None]:
                schedule_feedback_check(action_target, action, env.now, network)
                # Register with scorer for LLM vs Rule comparison
                rule_action, _ = _app_rules.get_rule_suggestion(network, event)
                decision_id = _app_scorer.register_decision(
                    current_time=env.now, action=action, target=action_target,
                    source="llm", failure_type=event.failure_type,
                    network=network, rule_suggestion=rule_action,
                )
                _app_pending_scores[decision_id] = event

        # Feedback follow-ups
        follow_ups = run_feedback_checks(env.now, network)
        if ENABLE_FEEDBACK_FOLLOWUPS:
            for follow_up in follow_ups:
                action = follow_up["action"]
                target = follow_up["node_id"]
                reason = follow_up.get("reason", "Feedback follow-up")

                result = execute_action(network, action, target, None)

                state["llm_decisions"].insert(0, {
                    "time": round(env.now, 1),
                    "failure": f"feedback follow-up on {target}",
                    "decision": f"Follow-up action: {action} — {reason} → Result: {result}",
                    "action": action,
                })
                state["llm_decisions"] = state["llm_decisions"][:15]

                add_event(f"Follow-up: {action} on {target}", "feedback")
                add_event(str(result), "success")
        else:
            for follow_up in follow_ups:
                add_event(
                    f"Follow-up suggested: {follow_up['action']} on {follow_up['node_id']}",
                    "feedback"
                )

        
        if ENABLE_OPTIMIZATION and env.now >= next_optimize:
            next_optimize += OPTIMIZE_EVERY
            suggestion = optimize_cluster(network)
            state["llm_decisions"].insert(0, {
                "time": round(env.now, 1),
                "failure": "Optimization Request",
                "decision": suggestion,
                "action": "optimize",
            })
            state["llm_decisions"] = state["llm_decisions"][:15]
            add_event(f"LLM optimization at t={env.now:.0f}", "llm")

        
        alerts = predictor.check_all_nodes(network, env.now)
        for alert in alerts:
            existing = [p for p in state["predictions"] if p["node_id"] == alert["node_id"]]
            if existing:
                existing[0].update({
                    "ticks": alert["ticks"],
                    "confidence": alert["confidence"],
                    "health": alert["health"],
                    "time": round(env.now, 1),
                })
            else:
                state["predictions"].insert(0, {
                    "node_id": alert["node_id"],
                    "ticks": alert["ticks"],
                    "confidence": alert["confidence"],
                    "health": alert["health"],
                    "time": round(env.now, 1),
                })
                state["predictions"] = state["predictions"][:10]
                add_event(
                    f"PREDICTION: {alert['node_id']} will fail in "
                    f"~{alert['ticks']} ticks ({alert['confidence']}% conf)",
                    "predict"
                )

            
            if ENABLE_PREDICTIVE_ACTIONS and alert["ticks"] <= 5:
                handle_predictive_alert(alert, network, env.now)


        ready_ids = [d_id for d_id in list(_app_pending_scores.keys())
                     if _app_scorer.is_ready_to_score(d_id, env.now)]
        for d_id in ready_ids:
            fe = _app_pending_scores.pop(d_id)
            resolved = getattr(fe, "resolved", False)
            score = _app_scorer.score_outcome(d_id, env.now, network, resolved=resolved)
            if score:
                state["score_history"].append({
                    "time": round(score.get("scored_at", env.now), 1),
                    "source": score["source"],
                    "total_score": score["total_score"],
                    "speed": score["speed_score"],
                    "stability": score["stability_score"],
                    "cascade": score["cascade_score"],
                    "delivery": score["delivery_score"],
                    "action": score["action"],
                    "target": score["target"],
                    "resolved": score["resolved"],
                })
                state["score_history"] = state["score_history"][-50:]

        update_nodes(network)

def status_loop(env, network):
    while True:
        yield env.timeout(5)
        update_nodes(network)
        stats = network.get_stats()
        add_event(
            f"t={env.now:.0f} | "
            f"{stats.get('delivered', 0)} delivered | "
            f"{stats.get('dropped', 0)} dropped | "
            f"{stats.get('success_rate', 0)} success",
            "info"
        )

def run_simulation(cluster_size=5):
    global _predictive_actions_taken

    
    state["nodes"] = []
    state["stats"] = {}
    state["events"] = []
    state["llm_decisions"] = []
    state["predictions"] = []
    state["score_history"] = []
    state["sim_time"] = 0
    state["running"] = True
    state["complete"] = False
    state["cluster_size"] = cluster_size
    state["final_success_rate"] = None
    state["final_delivered"] = 0
    state["final_dropped"] = 0

    _predictive_actions_taken = set()

    
    reset_llm_state()
    reset_feedback_state()
    predictor._history = {}
    predictor._alert_state = {}

    env = simpy.Environment()
    config = generate_cluster(cluster_size)
    network = build_cluster(env, config)
    injector = FaultInjector(env, network)

    generate_failures(injector, cluster_size)

    add_event(f"✓ Cluster started with {cluster_size} nodes", "success")
    add_event("✓ Self-healing LLM engine active", "success")
    add_event("✓ Predictive failure detection active", "success")
    if not ENABLE_PREDICTIVE_ACTIONS:
        add_event("✓ Predictive actions disabled for debugging", "info")
    if not ENABLE_OPTIMIZATION:
        add_event("✓ Optimization disabled for debugging", "info")

    env.process(llm_decision_loop(env, network, injector))
    env.process(status_loop(env, network))

    while env.peek() < SIMULATION_TIME:
        env.step()
        time.sleep(0.01)

    final_stats = network.get_stats()
    state["final_success_rate"] = final_stats.get("success_rate", 0)
    state["final_delivered"] = final_stats.get("delivered", 0)
    state["final_dropped"] = final_stats.get("dropped", 0)

    report = analyze_simulation(network, injector.history)
    state["llm_decisions"].insert(0, {
        "time": SIMULATION_TIME,
        "failure": "Final Analysis",
        "decision": report,
        "action": "analyze",
    })
    state["llm_decisions"] = state["llm_decisions"][:15]

    state["sim_time"] = SIMULATION_TIME

    add_event(
        f"FINAL | {state['final_delivered']} delivered | "
        f"{state['final_dropped']} dropped | "
        f"{state['final_success_rate']} success",
        "success"
    )
    add_event("Simulation complete!", "success")
    state["complete"] = True
    state["running"] = False

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/state")
def get_state():
    return jsonify(state)

@app.route("/api/scores")
def get_scores():
    """Return score history for LLM vs Rule visualization chart."""
    return jsonify(state.get("score_history", []))

@app.route("/api/start", methods=["POST"])
def start_simulation():
    global _sim_thread
    if state["running"]:
        return jsonify({"error": "Simulation already running"}), 400

    size = int(request.json.get("cluster_size", 5))
    _sim_thread = threading.Thread(target=run_simulation, args=(size,), daemon=True)
    _sim_thread.start()
    return jsonify({"status": "started", "cluster_size": size})

if __name__ == "__main__":
    _sim_thread = threading.Thread(target=run_simulation, args=(5,), daemon=True)
    _sim_thread.start()
    print("✓ Dashboard running at http://localhost:8080")
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
