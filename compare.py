
import random
import simpy

from network import build_cluster
from failures import FaultInjector
from llm_agent import decide_and_recover, reset_llm_state
from rule_based import RuleBasedRecovery
from cluster_config import generate_cluster, generate_failures
from event_bus import bus
from scorer import scorer

SIMULATION_TIME = 60

def reset_all_state():
    bus.reset()
    reset_llm_state()
    scorer.reset()

def run_llm_simulation(cluster_size=5, seed=5001):
    """Run simulation with LLM recovery."""
    reset_all_state()
    random.seed(seed)

    env      = simpy.Environment()
    config   = generate_cluster(cluster_size)
    network  = build_cluster(env, config)
    injector = FaultInjector(env, network)
    generate_failures(injector, cluster_size)

    seen_failures = set()
    decisions     = []

    def llm_loop(env):
        while True:
            yield env.timeout(0.5)
            for failure in injector.history:
                event_id = f"{failure.failure_type}-{failure.target}-{failure.time:.1f}"
                if event_id not in seen_failures:
                    seen_failures.add(event_id)
                    decision, action, target = decide_and_recover(network, failure, env.now)
                    decisions.append({
                        "time":     env.now,
                        "failure":  failure.failure_type,
                        "target":   failure.target,
                        "action":   action,
                        "acted_on": target,
                        "decision": decision
                    })

    env.process(llm_loop(env))
    env.run(until=SIMULATION_TIME)

    stats = network.get_stats()
    return {
        "seed":      seed,
        "stats":     stats,
        "decisions": decisions,
        "history":   injector.history,
        "network":   network,
        "mode":      "LLM (Groq Llama 3.3-70b)"
    }

def run_rule_simulation(cluster_size=5, seed=5001):
    """Run simulation with rule-based recovery."""
    reset_all_state()
    random.seed(seed)

    env      = simpy.Environment()
    config   = generate_cluster(cluster_size)
    network  = build_cluster(env, config)
    injector = FaultInjector(env, network)
    generate_failures(injector, cluster_size)

    rules         = RuleBasedRecovery()
    seen_failures = set()
    decisions     = []

    def rule_loop(env):
        while True:
            yield env.timeout(0.5)
            for failure in injector.history:
                event_id = f"{failure.failure_type}-{failure.target}-{failure.time:.1f}"
                if event_id not in seen_failures:
                    seen_failures.add(event_id)
                    decision, action = rules.decide(network, failure, env.now)
                    decisions.append({
                        "time":     env.now,
                        "failure":  failure.failure_type,
                        "target":   failure.target,
                        "action":   action,
                        "decision": decision
                    })

    env.process(rule_loop(env))
    env.run(until=SIMULATION_TIME)

    stats = network.get_stats()
    return {
        "seed":      seed,
        "stats":     stats,
        "decisions": decisions,
        "history":   injector.history,
        "network":   network,
        "mode":      "Rule-Based (Hardcoded)"
    }

def print_comparison(llm_result, rule_result):
    """Print side-by-side comparison."""
    print("\n")
    print("=" * 72)
    print("  LLM vs RULE-BASED RECOVERY COMPARISON")
    print("=" * 72)
    print(f"  Matched seed: {llm_result['seed']}")

    l = llm_result["stats"]
    r = rule_result["stats"]

    print(f"\n{'Metric':<30} {'LLM':<20} {'Rule-Based':<20}")
    print("-" * 72)
    print(f"{'Total messages':<30} {l['total_messages']:<20} {r['total_messages']:<20}")
    print(f"{'Delivered':<30} {l['delivered']:<20} {r['delivered']:<20}")
    print(f"{'Dropped':<30} {l['dropped']:<20} {r['dropped']:<20}")
    print(f"{'Success rate':<30} {l['success_rate']:<20} {r['success_rate']:<20}")

    l_actions = len(llm_result["decisions"])
    r_actions = len(rule_result["decisions"])
    print(f"{'Recovery actions taken':<30} {l_actions:<20} {r_actions:<20}")

    l_resolved = sum(1 for f in llm_result["history"] if f.resolved)
    r_resolved = sum(1 for f in rule_result["history"] if f.resolved)
    l_total    = len(llm_result["history"])
    r_total    = len(rule_result["history"])
    print(f"{'Failures resolved':<30} {f'{l_resolved}/{l_total}':<20} {f'{r_resolved}/{r_total}':<20}")

    
    def avg_mttr(history):
        vals = [f.resolved_at - f.time for f in history if f.resolved and f.resolved_at]
        return round(sum(vals) / len(vals), 1) if vals else None

    l_mttr = avg_mttr(llm_result["history"])
    r_mttr = avg_mttr(rule_result["history"])
    l_mttr_str = f"{l_mttr} ticks" if l_mttr else "N/A"
    r_mttr_str = f"{r_mttr} ticks" if r_mttr else "N/A"
    print(f"{'Avg MTTR':<30} {l_mttr_str:<20} {r_mttr_str:<20}")

    l_healthy = sum(1 for n in llm_result["network"].nodes.values() if n.status == "healthy")
    r_healthy = sum(1 for n in rule_result["network"].nodes.values() if n.status == "healthy")
    total     = len(llm_result["network"].nodes)
    print(f"{'Final healthy nodes':<30} {f'{l_healthy}/{total}':<20} {f'{r_healthy}/{total}':<20}")

    print("\n")
    print("─" * 72)
    print("  RECOVERY ACTIONS COMPARISON")
    print("─" * 72)

    l_counts = {}
    r_counts = {}
    for d in llm_result["decisions"]:
        l_counts[d["action"]] = l_counts.get(d["action"], 0) + 1
    for d in rule_result["decisions"]:
        r_counts[d["action"]] = r_counts.get(d["action"], 0) + 1

    all_actions = set(list(l_counts.keys()) + list(r_counts.keys()))
    for action in sorted(all_actions):
        l_count = l_counts.get(action, 0)
        r_count = r_counts.get(action, 0)
        print(f"  {action:<28} {str(l_count)+'x':<20} {str(r_count)+'x':<20}")

    print("\n")
    print("─" * 72)
    print("  VERDICT")
    print("─" * 72)

    l_rate = float(l['success_rate'].replace('%', ''))
    r_rate = float(r['success_rate'].replace('%', ''))
    diff   = l_rate - r_rate

    if diff > 0:
        print(f"\n  LLM wins by {diff:.1f}% higher delivery rate on the same seed")
    elif diff < 0:
        print(f"\n  Rule-based wins by {abs(diff):.1f}% higher delivery rate on the same seed")
    else:
        print(f"\n  Tie on delivery rate for this seed")

    
    print("\n")
    print("─" * 72)
    print("  PER-FAILURE-TYPE MTTR")
    print("─" * 72)

    from collections import defaultdict
    def _mttr_by_type(history):
        by_type = defaultdict(list)
        for f in history:
            if f.resolved and f.resolved_at:
                by_type[f.failure_type].append(f.resolved_at - f.time)
        return {k: round(sum(v)/len(v), 1) for k, v in by_type.items()}

    l_mttr_by_type = _mttr_by_type(llm_result["history"])
    r_mttr_by_type = _mttr_by_type(rule_result["history"])
    all_types = sorted(set(list(l_mttr_by_type.keys()) + list(r_mttr_by_type.keys())))

    print(f"  {'Failure Type':<24} {'LLM MTTR':<16} {'Rule MTTR':<16} {'Winner':<12}")
    print("  " + "-" * 68)
    for ftype in all_types:
        lv = l_mttr_by_type.get(ftype)
        rv = r_mttr_by_type.get(ftype)
        lstr = f"{lv} ticks" if lv is not None else "N/A"
        rstr = f"{rv} ticks" if rv is not None else "N/A"
        if lv is not None and rv is not None:
            winner = "LLM" if lv < rv else ("Rules" if rv < lv else "Tie")
        else:
            winner = "-"
        print(f"  {ftype:<24} {lstr:<16} {rstr:<16} {winner:<12}")

    print("\n" + "=" * 72)

if __name__ == "__main__":
    import sys
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else size * 1000 + 1

    print(f" Running LLM simulation ({size} nodes, seed={seed})...")
    llm_result = run_llm_simulation(size, seed=seed)
    print(f"LLM done — {llm_result['stats']['success_rate']} success rate")

    print(f"\n Running Rule-based simulation ({size} nodes, seed={seed})...")
    rule_result = run_rule_simulation(size, seed=seed)
    print(f" Rules done — {rule_result['stats']['success_rate']} success rate")

    print_comparison(llm_result, rule_result)
