import random, simpy, json, sys, time
from network import build_cluster
from failures import FaultInjector
from cluster_config import generate_cluster, generate_failures
from event_bus import bus
from rule_based import RuleBasedRecovery
from llm_agent import reset_llm_state, execute_action, validate_action, record_action, decide_only
from feedback import reset_feedback_state
from scorer import DecisionScorer

SIMULATION_TIME = 70


def reset_all():
    bus.reset()
    reset_llm_state()
    try:
        reset_feedback_state()
    except Exception:
        pass


def run_single_sim(cluster_size, seed, use_llm=True):
    reset_all()
    random.seed(seed)
    env = simpy.Environment()
    config = generate_cluster(cluster_size)
    network = build_cluster(env, config)
    injector = FaultInjector(env, network)
    generate_failures(injector, cluster_size)
    rules = RuleBasedRecovery()
    scorer = DecisionScorer()
    seen = set()
    decisions_made = []

    def decision_loop(env):
        while True:
            yield env.timeout(0.5)
            for failure in injector.history:
                eid = f"{failure.failure_type}-{failure.target}-{failure.time:.1f}"
                if eid in seen:
                    continue
                seen.add(eid)
                confidence, tier, reason = rules.assess_confidence(network, failure)
                if tier == 1 or not use_llm:
                    decision_str, action = rules.decide(network, failure, env.now)
                    record_action(env.now, action, failure.target, decision_str, source="rule")
                    did = scorer.register_decision(
                        env.now, action, failure.target, "rule",
                        failure.failure_type, network
                    )
                    decisions_made.append({
                        "time": env.now, "action": action,
                        "target": failure.target, "source": "rule",
                        "decision_id": did
                    })
                else:
                    llm_decision = decide_only(network, failure, env.now)
                    action = llm_decision.get("action", "monitor_only")
                    target = llm_decision.get("target", failure.target)
                    diverges = llm_decision.get("diverges_from_rule", False)
                    valid, vmsg = validate_action(action, target, network, failure.failure_type)
                    if not valid:
                        action = "monitor_only"
                    result = execute_action(network, action, target, failure, env.now)
                    record_action(env.now, action, target, result, source="llm")
                    rule_sugg, _ = rules.get_rule_suggestion(network, failure)
                    did = scorer.register_decision(
                        env.now, action, target, "llm",
                        failure.failure_type, network,
                        rule_suggestion=rule_sugg
                    )
                    decisions_made.append({
                        "time": env.now, "action": action,
                        "target": target, "source": "llm",
                        "diverges": diverges, "decision_id": did
                    })
            for d in decisions_made:
                did = d.get("decision_id")
                if did and scorer.is_ready_to_score(did, env.now):
                    target = d["target"]
                    node = network.nodes.get(target)
                    if node:
                        resolved = node.status in ("healthy", "degraded")
                    elif "<->" in target:
                        parts = target.split("<->")
                        resolved = (
                            (parts[0], parts[1]) not in network.partitions
                            and (parts[1], parts[0]) not in network.partitions
                        )
                    elif target == "network":
                        resolved = not getattr(network, "spike_active", False)
                    else:
                        resolved = True
                    scorer.score_outcome(did, env.now, network, resolved=resolved)
                    d["decision_id"] = None

    env.process(decision_loop(env))
    env.run(until=SIMULATION_TIME)
    for d in decisions_made:
        did = d.get("decision_id")
        if did:
            scorer.score_outcome(did, SIMULATION_TIME, network, resolved=True)
    mttr_vals = [
        f.resolved_at - f.time
        for f in injector.history if f.resolved and f.resolved_at
    ]
    avg_mttr = round(sum(mttr_vals) / len(mttr_vals), 2) if mttr_vals else None
    resolved = sum(1 for f in injector.history if f.resolved)
    total = len(injector.history)
    net_stats = network.get_stats()
    summary = scorer.get_summary()
    overrides = sum(1 for d in decisions_made if d.get("diverges"))
    return {
        "seed": seed, "cluster_size": cluster_size, "use_llm": use_llm,
        "failures_total": total, "failures_resolved": resolved,
        "avg_mttr": avg_mttr, "delivery_rate": net_stats["success_rate"],
        "commit_rate": net_stats.get("commit_rate", "0%"),
        "timeout_rate": net_stats.get("timeout_rate", "0%"),
        "stale_read_rate": net_stats.get("stale_read_rate", "0%"),
        "quorum_unavailable_ticks": net_stats.get("quorum_unavailable_ticks", 0),
        "leader_elections": net_stats.get("leader_elections", 0),
        "llm_overrides": overrides, "scorer_summary": summary,
        "decisions_count": len(decisions_made),
    }


def _safe_avg(vals):
    clean = [v for v in vals if v is not None]
    return round(sum(clean) / len(clean), 2) if clean else 0


def run_benchmark(sizes=None, seeds_per_size=5, base_seed=42):
    if sizes is None:
        sizes = [5, 10, 20]
    results = []
    for size in sizes:
        print(f"\n{'=' * 60}")
        print(f"  BENCHMARK: {size} nodes, {seeds_per_size} seeds")
        print(f"{'=' * 60}")
        for mode_label, use_llm in [("LLM+Rules", True), ("Rules Only", False)]:
            mode_results = []
            for i in range(seeds_per_size):
                seed = base_seed + size * 100 + i
                t0 = time.time()
                r = run_single_sim(size, seed, use_llm=use_llm)
                elapsed = time.time() - t0
                mode_results.append(r)
                print(
                    f"  [{mode_label}] seed={seed}: "
                    f"MTTR={r['avg_mttr']}, "
                    f"resolved={r['failures_resolved']}/{r['failures_total']}, "
                    f"delivery={r['delivery_rate']}, "
                    f"commit={r['commit_rate']}, "
                    f"timeout={r['timeout_rate']}, "
                    f"stale={r['stale_read_rate']}, "
                    f"overrides={r['llm_overrides']} "
                    f"({elapsed:.1f}s)"
                )
            avg_mttr = _safe_avg([r["avg_mttr"] for r in mode_results])
            avg_resolved = _safe_avg([
                r["failures_resolved"] / r["failures_total"] * 100
                if r["failures_total"] > 0 else 100
                for r in mode_results
            ])
            avg_overrides = _safe_avg([r["llm_overrides"] for r in mode_results])
            all_llm_scores, all_rule_scores = [], []
            for r in mode_results:
                s = r.get("scorer_summary", {})
                if "llm" in s:
                    all_llm_scores.append(s["llm"]["avg_total_score"])
                if "rule" in s:
                    all_rule_scores.append(s["rule"]["avg_total_score"])
            avg_llm_score = _safe_avg(all_llm_scores) if all_llm_scores else None
            avg_rule_score = _safe_avg(all_rule_scores) if all_rule_scores else None
            avg_commit = _safe_avg([float(str(r["commit_rate"]).rstrip("%")) for r in mode_results])
            avg_timeout = _safe_avg([float(str(r["timeout_rate"]).rstrip("%")) for r in mode_results])
            avg_stale = _safe_avg([float(str(r["stale_read_rate"]).rstrip("%")) for r in mode_results])
            avg_quorum_loss = _safe_avg([r["quorum_unavailable_ticks"] for r in mode_results])
            avg_elections = _safe_avg([r["leader_elections"] for r in mode_results])
            agg = {
                "cluster_size": size, "mode": mode_label, "seeds": seeds_per_size,
                "avg_mttr": avg_mttr,
                "avg_resolved_pct": round(avg_resolved, 1),
                "avg_overrides": round(avg_overrides, 1),
                "avg_commit_rate": round(avg_commit, 1),
                "avg_timeout_rate": round(avg_timeout, 1),
                "avg_stale_read_rate": round(avg_stale, 1),
                "avg_quorum_unavailable_ticks": round(avg_quorum_loss, 1),
                "avg_leader_elections": round(avg_elections, 1),
                "avg_llm_score": round(avg_llm_score, 1) if avg_llm_score else None,
                "avg_rule_score": round(avg_rule_score, 1) if avg_rule_score else None,
            }
            results.append(agg)
            print(f"\n  [{mode_label}] AVERAGE over {seeds_per_size} seeds:")
            print(f"    MTTR={avg_mttr}, resolved={avg_resolved:.1f}%, overrides={avg_overrides:.1f}")
            if avg_llm_score:
                print(f"    LLM score={avg_llm_score:.1f}, Rule score={avg_rule_score:.1f}")
    print(f"\n{'=' * 60}")
    print(f"  BENCHMARK SUMMARY")
    print(f"{'=' * 60}")
    header = f"  {'Size':<6} {'Mode':<14} {'MTTR':<6} {'Resolved%':<10} {'Commit%':<8} {'Timeout%':<9} {'Stale%':<7} {'QuorumLoss':<11} {'Overrides':<10}"
    print(header)
    print(f"  {'-' * 100}")
    for r in results:
        print(
            f"  {r['cluster_size']:<6} {r['mode']:<14} {r['avg_mttr']:<6} "
            f"{r['avg_resolved_pct']:<10} {r['avg_commit_rate']:<8} {r['avg_timeout_rate']:<9} "
            f"{r['avg_stale_read_rate']:<7} {r['avg_quorum_unavailable_ticks']:<11} {r['avg_overrides']:<10}"
        )
        llm_s = str(r["avg_llm_score"]) if r["avg_llm_score"] else "N/A"
        rule_s = str(r["avg_rule_score"]) if r["avg_rule_score"] else "N/A"
        print(f"    scores -> LLM={llm_s}, Rules={rule_s}, leader_elections={r['avg_leader_elections']}")
    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to benchmark_results.json")
    return results


if __name__ == "__main__":
    sizes = [5, 10, 20]
    seeds = 3
    if len(sys.argv) > 1:
        sizes = [int(x) for x in sys.argv[1].split(",")]
    if len(sys.argv) > 2:
        seeds = int(sys.argv[2])
    print(f"Running benchmark: sizes={sizes}, seeds_per_size={seeds}")
    run_benchmark(sizes=sizes, seeds_per_size=seeds)
