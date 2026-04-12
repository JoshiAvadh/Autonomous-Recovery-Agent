import sys
from datetime import datetime

import simpy
import cluster_config

from failures import FaultInjector, bus
from llm_agent import (
    decide_and_recover,
    decide_batch,
    record_action,
    analyze_simulation,
    get_metrics,
    reset_llm_state,
    get_pending_restarts,
    is_obvious_case,
    execute_action,
    check_load_shedding,
    optimize_cluster,
)
from network import build_cluster
from rule_based import RuleBasedRecovery
from scorer import DecisionScorer
from predictor import predictor
from decision_memory import DecisionMemory
from feedback import (
    reset_feedback_state,
    schedule_feedback_check,
    run_feedback_checks,
)


class SimulationLogger:
    """
    Two-tier logger:
      log()             → file only (verbose detail)
      log_console()     → both terminal AND file (key events)
      log_llm_decision  → compact one-liner on terminal, full block in file
    """

    def __init__(self):
        self.started_at = datetime.now()
        self.lines = []  

    def log(self, text):
        """Write to file only — keeps terminal clean."""
        self.lines.append(text)

    def log_console(self, text):
        """Write to both terminal and file — for key events."""
        print(text)
        self.lines.append(text)

    def log_llm_decision(self, sim_time, failure_type, target, summary):
       
        short = summary.split(" -> Result: ")
        action_part = short[0][:120]
        result_part = short[1][:60] if len(short) > 1 else ""
        result_suffix = f" → {result_part}" if result_part else ""
        print(f"  [t={sim_time:.1f}] {failure_type} on {target}: {action_part}{result_suffix}")

        
        self.lines.append(f"\n-- LLM Decision at t={sim_time:.1f} ---------")
        self.lines.append(f"  Failure : {failure_type} on {target}")
        self.lines.append(f"  Decision:")
        self.lines.append(f"    {summary}")

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("  DISTRIBUTED SYSTEM SIMULATION LOG\n")
            f.write(f"  Started: {self.started_at.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n")
            f.write("\n".join(self.lines))
            f.write("\n")


def load_cluster_config(num_nodes):
    if hasattr(cluster_config, "generate_cluster"):
        return cluster_config.generate_cluster(num_nodes)
    raise AttributeError("cluster_config.py must define generate_cluster(size)")


def schedule_failures(injector, num_nodes):
    if hasattr(cluster_config, "generate_failures"):
        return cluster_config.generate_failures(injector, num_nodes)
    raise AttributeError("cluster_config.py must define generate_failures(injector, cluster_size)")


def print_cluster_status(logger, env, network):
    # Import here to avoid circular import at module level
    from llm_agent import _shed_nodes

    logger.log("\n==================================================")
    logger.log(f"[t={env.now:.1f}] Cluster Status:")

    if _shed_nodes:
        logger.log(f"  ** LOAD SHEDDING ACTIVE — shed nodes: {sorted(_shed_nodes)} **")

    for node_id, node in sorted(network.nodes.items(), key=lambda kv: kv[0]):
        extra = ""
        if hasattr(node, "queue_length"):
            extra += f" | q={node.queue_length}"
        if getattr(node, "replica_lag", None) is not None:
            extra += f" | lag={node.replica_lag}"
        shed_tag = " [SHED]" if node_id in _shed_nodes else ""
        logger.log(
            f"  {node_id} ({node.role}): {node.status} | "
            f"health={node.health:.0f}% | load={node.load:.0f}%{extra}{shed_tag}"
        )
    logger.log("==================================================")


def print_final_node_states(logger, network):
    from llm_agent import _shed_nodes

    logger.log("\n── Final Node States ─────────────────────────────────")
    for node_id, node in sorted(network.nodes.items(), key=lambda kv: kv[0]):
        suffix = ""
        if node_id in _shed_nodes:
            suffix = " [SHED]"
        elif node.health >= 100:
            suffix = " [FULLY RESTORED]"
        logger.log(
            f"  {node_id} ({node.role:<8}): status={node.status:<10} "
            f"health={node.health:.0f}% load={node.load:.0f}%{suffix}"
        )


def print_final_network_stats(logger, network):
    net_stats = network.get_stats()

    logger.log("\n── Final Network Stats ───────────────────────────────")
    for k in [
        "total_messages",
        "delivered",
        "dropped",
        "success_rate",
        "requests_total",
        "requests_completed",
        "request_timeouts",
        "timeout_events",
        "timeout_rate",
        "writes_attempted",
        "writes_committed",
        "commit_rate",
        "reads_completed",
        "stale_reads",
        "stale_read_rate",
        "request_retries",
        "quorum_available_ticks",
        "quorum_unavailable_ticks",
        "leader_elections",
        "split_brain_events",
        "current_primary",
        "commit_index",
    ]:
        if k in net_stats:
            logger.log(f"  {k}: {net_stats[k]}")


def summarize_failures(logger, injector):
    hist = injector.history
    resolved = [f for f in hist if getattr(f, "resolved", False) and getattr(f, "resolved_at", None) is not None]
    active = [f for f in hist if not getattr(f, "resolved", False)]

    logger.log("\n── Failure History ────────")
    for f in hist:
        if getattr(f, "resolved", False) and getattr(f, "resolved_at", None) is not None:
            mttr = f.resolved_at - f.time
            logger.log(
                f"  [{f.time:5.1f}] {f.failure_type:<20} on {f.target:<12} "
                f"(resolved@t={f.resolved_at:.1f}, MTTR={mttr:.1f})"
            )
        else:
            logger.log(
                f"  [{f.time:5.1f}] {f.failure_type:<20} on {f.target:<12} "
                f"(ACTIVE)"
            )

    logger.log(f"\n  Total: {len(hist)} | Active: {len(active)} | Resolved: {len(resolved)}")

    if resolved:
        mttrs = [f.resolved_at - f.time for f in resolved]
        avg_mttr = sum(mttrs) / len(mttrs)
        logger.log(f"\n  Avg MTTR: {avg_mttr:.1f} ticks across {len(resolved)} resolved failures")


def print_score_summary(logger, scorer):
    logger.log_console("\n" + scorer.get_comparison_report())

    summary = scorer.get_summary()
    if not summary:
        return

    logger.log_console("")
    logger.log_console("  SCORE SUMMARY BY SOURCE")
    logger.log_console("")
    for source in sorted(summary.keys()):
        data = summary[source]
        logger.log_console(
            f"  {source:<12} count={data.get('count', 0):<3} "
            f"avg_total={data.get('avg_total_score', 'N/A'):<5} "
            f"avg_mttr={data.get('avg_mttr', 'N/A'):<5} "
            f"resolved={data.get('resolved_pct', 'N/A')}% "
            f"diverged={data.get('diverged_count', 0)}"
        )
    logger.log_console("")


def print_simulation_metrics(logger, injector, network):
    llm = get_metrics()
    net_stats = network.get_stats()

    resolved = [f for f in injector.history if getattr(f, "resolved", False) and getattr(f, "resolved_at", None) is not None]
    avg_mttr = 0.0
    if resolved:
        avg_mttr = sum((f.resolved_at - f.time) for f in resolved) / len(resolved)

    logger.log_console("")
    logger.log_console("  SIMULATION METRICS")
    logger.log_console("")
    logger.log_console(f"  Total LLM decisions:    {llm.get('total_decisions', 0)}")
    logger.log_console(f"  LLM novel overrides:    {llm.get('llm_overrides', 0)}")
    logger.log_console(f"  Avg MTTR:               {avg_mttr:.1f} ticks")
    logger.log_console(f"  Failures resolved:      {len(resolved)}/{len(injector.history)}")
    logger.log_console(f"  Message delivery:       {net_stats.get('success_rate', '0%')}")
    logger.log_console(
        f"  Commit rate:            {net_stats.get('commit_rate', '0%')} "
        f"({net_stats.get('writes_committed', 0)}/{net_stats.get('writes_attempted', 0)})"
    )
    logger.log_console(
        f"  Timeout rate:           {net_stats.get('timeout_rate', '0%')} "
        f"({net_stats.get('request_timeouts', 0)} timeouts)"
    )
    logger.log_console(f"  Quorum unavailable:     {net_stats.get('quorum_unavailable_ticks', 0):.1f} ticks")
    logger.log_console(f"  Leader elections:       {net_stats.get('leader_elections', 0)}")
    logger.log_console(f"  Split-brain events:     {net_stats.get('split_brain_events', 0)}")

    
    shed_nodes = llm.get("shed_nodes", [])
    shedding_active = llm.get("shedding_active", False)
    if shed_nodes or shedding_active:
        logger.log_console(f"  Load shedding active:   {shedding_active}")
        logger.log_console(f"  Currently shed nodes:   {shed_nodes}")

    
    logger.log(f"  Rule echoes:            {llm.get('rule_echoes', 0)}")
    logger.log(f"  Timeout events:         {net_stats.get('timeout_events', 0)}")
    logger.log(
        f"  Stale read rate:        {net_stats.get('stale_read_rate', '0%')} "
        f"({net_stats.get('stale_reads', 0)} stale reads)"
    )

    logger.log_console("")


def _finalize_post_isolation(env, node, node_id, logger):
    yield env.timeout(3.0)
    if node.status == "degraded":
        node.status = "healthy"
        node.health = 80.0
        logger.log(f"[t={env.now:.1f}] POST-ISOLATION HEALTHY: {node_id} fully recovered as {node.role}")
        logger.log_llm_decision(
            env.now,
            "post_isolation_recovery",
            node_id,
            f"[AUTO-RECOVERY] {node_id} fully healthy post-isolation"
        )

        while node.health < 100.0 and node.status == "healthy":
            yield env.timeout(1.0)
            if node.status != "healthy":
                break
            node.health = min(100.0, node.health + 5.0)
            if node.health >= 100.0:
                logger.log(f"[t={env.now:.1f}] RESTORED: {node_id} health fully restored to 100%")


def run_simulation(num_nodes=5):
    reset_llm_state()
    reset_feedback_state()
    bus.reset()

    env = simpy.Environment()
    logger = SimulationLogger()
    scorer = DecisionScorer()
    rules = RuleBasedRecovery()

    logger.log_console("")
    logger.log_console("  DISTRIBUTED SYSTEM SIMULATION LOG")
    logger.log_console(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log_console("")

    logger.log_console(f" Building cluster with {num_nodes} nodes...")
    cluster_cfg = load_cluster_config(num_nodes)
    network = build_cluster(env, cluster_cfg)

    logger.log("[INIT] LLM calls: synchronous (ensures decisions arrive at correct sim time)")

    injector = FaultInjector(env, network)
    schedule_failures(injector, num_nodes)

    logger.log_console(f" Starting self-healing simulation ({num_nodes} nodes)...\n")

    pending_failures = []
    pending_scores = {}

    SIM_END = 74.0
    STATUS_EVERY = 10.0
    next_status = 10.0
    OPTIMIZE_EVERY = 15.0
    next_optimize = 15.0

    
    total_shed_events = 0
    total_readmit_events = 0
    total_proactive_actions = 0
    total_predictive_actions = 0
    predictive_actions_taken = set()  

    
    memory = DecisionMemory()
    PREDICT_EVERY = 3.0
    next_predict = 3.0

    while env.now < SIM_END:
        env.step()

        while bus.has_events():
            event = bus.consume(timeout=0)
            if event is not None:
                pending_failures.append(event)

        if env.now >= next_status:
            print_cluster_status(logger, env, network)
            next_status += STATUS_EVERY

        
        if env.now >= next_optimize:
            next_optimize += OPTIMIZE_EVERY
            logger.log(f"\n[t={env.now:.1f}] Running proactive LLM optimization...")
            opt_result = optimize_cluster(network)
            if opt_result and "No optimization" not in opt_result:
                total_proactive_actions += 1
                logger.log(f"[t={env.now:.1f}] [PROACTIVE] {opt_result}")
                logger.log_llm_decision(
                    env.now, "proactive_optimization", "cluster", opt_result
                )
                record_action(env.now, "proactive_optimize", "cluster", opt_result, source="llm_proactive")

                decision_id = scorer.register_decision(
                    current_time=env.now,
                    action="proactive_optimize",
                    target="cluster",
                    source="llm_proactive",
                    failure_type="proactive",
                    network=network,
                    rule_suggestion=None,
                )
                
                class ProactiveEvent:
                    def __init__(self, time_now):
                        self.failure_type = "proactive"
                        self.target = "cluster"
                        self.time = time_now
                        self.resolved = True
                        self.resolved_at = time_now
                pending_scores[decision_id] = ProactiveEvent(env.now)
        

        
        if env.now >= next_predict:
            next_predict += PREDICT_EVERY
            alerts = predictor.check_all_nodes(network, env.now)
            for alert in alerts:
                node_id = alert["node_id"]
                if node_id in predictive_actions_taken:
                    continue
                if alert["ticks"] <= 6 and alert["confidence"] >= 40:
                    predictive_actions_taken.add(node_id)
                    total_predictive_actions += 1
                    node = network.nodes.get(node_id)
                    if not node:
                        continue

                    
                    if node.role in ("Primary", "Replica") and alert["ticks"] <= 3:
                        p_action = "reroute_traffic"
                        p_reason = f"Predicted failure in {alert['ticks']} ticks, rerouting to protect consensus"
                    elif node.health < 40:
                        p_action = "restart_node"
                        p_reason = f"Health at {node.health:.0f}%, predicted failure in {alert['ticks']} ticks"
                    else:
                        p_action = "reroute_traffic"
                        p_reason = f"Preventive reroute: health declining, failure in ~{alert['ticks']} ticks"

                    class PredictiveEvent:
                        def __init__(self, target, time_now):
                            self.failure_type = "predicted_failure"
                            self.target = target
                            self.time = time_now
                            self.resolved = True
                            self.resolved_at = time_now

                    fake_event = PredictiveEvent(node_id, env.now)
                    result = execute_action(network, p_action, node_id, fake_event, env.now)
                    record_action(env.now, p_action, node_id, result, source="llm_predictive")

                    summary = (
                        f"[PREDICTIVE] {p_action} on {node_id} — {p_reason} "
                        f"(confidence={alert['confidence']}%) → {result}"
                    )
                    logger.log(f"\n[t={env.now:.1f}] {summary}")
                    logger.log_llm_decision(env.now, "predicted_failure", node_id, summary)

                    decision_id = scorer.register_decision(
                        current_time=env.now,
                        action=p_action,
                        target=node_id,
                        source="llm_predictive",
                        failure_type="predicted_failure",
                        network=network,
                        rule_suggestion=None,
                    )
                    pending_scores[decision_id] = fake_event
        
        shed_actions = check_load_shedding(network, env.now)
        for description, action_type, target in shed_actions:
            logger.log(f"\n[t={env.now:.1f}] {description}")
            logger.log_llm_decision(
                env.now,
                action_type,
                target,
                description,
            )

            if action_type == "load_shed":
                total_shed_events += 1
            elif action_type == "load_shed_readmit":
                total_readmit_events += 1

            
            decision_id = scorer.register_decision(
                current_time=env.now,
                action=action_type,
                target=target,
                source="llm_shed",
                failure_type="congestion",
                network=network,
                rule_suggestion=None,
            )

            
            class ShedEvent:
                def __init__(self, target, time_now):
                    self.failure_type = "congestion"
                    self.target = target
                    self.time = time_now
                    self.resolved = action_type == "load_shed_readmit"
                    self.resolved_at = time_now if self.resolved else None

                def resolve(self, resolved_at):
                    self.resolved = True
                    self.resolved_at = resolved_at

            pending_scores[decision_id] = ShedEvent(target, env.now)
        
        due_restarts = get_pending_restarts(env.now)
        for node_id in due_restarts:
            
            from llm_agent import _shed_nodes
            if node_id in _shed_nodes:
                continue

            node = network.nodes.get(node_id)
            if node and node.status == "failed":
                logger.log(f"[t={env.now:.1f}] POST-ISOLATION RESTART: {node_id} coming back online")

                if hasattr(network, "rejoin_node"):
                    preferred_role = node.role
                    if node.role == "Primary":
                        current_primary = getattr(network, "current_primary", None)
                        if current_primary is not None and current_primary != node_id:
                            preferred_role = "Replica"

                    network.rejoin_node(
                        node_id,
                        preferred_role=preferred_role,
                        degraded=True,
                        health=30.0,
                        load=15.0,
                    )
                else:
                    for other_id in network.nodes:
                        if other_id != node_id:
                            network.heal_partition(node_id, other_id)
                    node.status = "degraded"
                    node.health = 30.0
                    node.load = 15.0

                result = f"Post-isolation: {node_id} restored to degraded, health=30%"
                record_action(env.now, "post_isolation_restart", node_id, result, source="auto_recovery")

                summary = f"[AUTO-RECOVERY] Post-isolation restart of {node_id} -- reconnected to cluster"
                logger.log_llm_decision(env.now, "post_isolation_restart", node_id, summary)

                env.process(_finalize_post_isolation(env, node, node_id, logger))

        
        follow_ups = run_feedback_checks(env.now, network)
        for follow_up in follow_ups:
            target = follow_up.get("node_id")
            action = follow_up.get("action", "monitor_only")
            reason = follow_up.get("reason", "Follow-up action")

            if not target or action in ["monitor_only", "none", "parse_failed"]:
                continue

            class FollowUpEvent:
                def __init__(self, target, time_now):
                    self.failure_type = "follow_up"
                    self.target = target
                    self.time = time_now
                    self.resolved = False
                    self.resolved_at = None

                def resolve(self, resolved_at):
                    self.resolved = True
                    self.resolved_at = resolved_at

            fake_event = FollowUpEvent(target, env.now)
            result = execute_action(network, action, target, fake_event, env.now)
            record_action(env.now, action, target, result, source="llm")

            summary = f"Follow-up: {action} on {target} -- {reason} -> {result}"
            logger.log(f"\n[t={env.now:.1f}] {summary}")
            logger.log_llm_decision(env.now, "follow_up", target, summary)

            decision_id = scorer.register_decision(
                current_time=env.now,
                action=action,
                target=target,
                source="llm",
                failure_type="follow_up",
                network=network,
                rule_suggestion=None,
            )
            pending_scores[decision_id] = fake_event
            schedule_feedback_check(target, action, env.now, network)

        
        ready_ids = [d_id for d_id in list(pending_scores.keys()) if scorer.is_ready_to_score(d_id, env.now)]
        for d_id in ready_ids:
            failure_event = pending_scores.pop(d_id)
            resolved = getattr(failure_event, "resolved", False)
            score = scorer.score_outcome(d_id, env.now, network, resolved=resolved)
            if score:
                logger.log(
                    f"[t={env.now:.1f}] [SCORE] {score['target']}: total={score['total_score']:.0f} "
                    f"(speed={score['speed_score']:.0f}, stability={score['stability_score']:.0f}, "
                    f"cascade={score['cascade_score']:.0f}, delivery={score['delivery_score']:.0f}) "
                    f"source={score['source']}"
                )
                
                node = network.nodes.get(score['target'])
                node_role = node.role if node else "unknown"
                concurrent = sum(1 for n in network.nodes.values() if n.status in ("failed", "degraded"))
                health_at = node.health if node else 0
                memory.record_outcome(
                    failure_type=score.get('failure_type', 'unknown'),
                    node_role=node_role,
                    action=score['action'],
                    source=score['source'],
                    score=score['total_score'],
                    resolved=resolved,
                    mttr=score.get('mttr'),
                    cluster_size=len(network.nodes),
                    concurrent_failures=concurrent,
                    health_at_decision=health_at,
                )

        if not pending_failures:
            continue

        failures_now = pending_failures[:]
        pending_failures.clear()

        if len(failures_now) > 1:
            for f in failures_now:
                logger.log(f"\n[t={env.now:.1f}] Failure detected: {f.failure_type} on {f.target}")

            batch_decisions = decide_batch(network, failures_now, env.now)

            for failure_event, decision in zip(failures_now, batch_decisions):
                action = decision.get("action", "monitor_only")
                target = decision.get("target", failure_event.target)
                reason = decision.get("reason", "No reason given")
                urgency = decision.get("urgency", "medium")
                confidence = decision.get("confidence", 0)
                diverges = decision.get("diverges_from_rule", False)

                result = execute_action(network, action, target, failure_event, env.now)
                record_action(env.now, action, target, result, source="llm")

                summary = (
                    f"[{urgency.upper()}] [{confidence}% confident]"
                    f"{' [LLM OVERRIDE]' if diverges else ''} "
                    f"Action: {action} on {target} -- {reason} -> Result: {result}"
                )

                logger.log(f"\n[t={env.now:.1f}] LLM Decision: {summary}")
                logger.log_llm_decision(env.now, failure_event.failure_type, failure_event.target, summary)

                rule_action, _ = rules.get_rule_suggestion(network, failure_event)
                decision_id = scorer.register_decision(
                    current_time=env.now,
                    action=action,
                    target=target,
                    source="llm",
                    failure_type=failure_event.failure_type,
                    network=network,
                    rule_suggestion=rule_action,
                )
                pending_scores[decision_id] = failure_event
                schedule_feedback_check(target, action, env.now, network)

            continue

        failure_event = failures_now[0]
        logger.log(f"\n[t={env.now:.1f}] Failure detected: {failure_event.failure_type} on {failure_event.target}")

        if is_obvious_case(failure_event, network):
            confidence, tier, reason = rules.assess_confidence(network, failure_event)
            logger.log(f"[t={env.now:.1f}] [ROUTER] confidence={confidence}% → TIER-1/RULE ({reason})")

            rule_action, rule_target = rules.get_rule_suggestion(network, failure_event)
            summary, action = rules.decide(network, failure_event, env.now)

            logger.log(f"[t={env.now:.1f}] Rule-Based Decision: {summary}")
            logger.log_llm_decision(env.now, failure_event.failure_type, failure_event.target, summary)

            record_action(env.now, action, rule_target, summary, source="rule")

            decision_id = scorer.register_decision(
                current_time=env.now,
                action=action,
                target=rule_target,
                source="rule",
                failure_type=failure_event.failure_type,
                network=network,
                rule_suggestion=rule_action,
            )
            pending_scores[decision_id] = failure_event
            schedule_feedback_check(rule_target, action, env.now, network)

        else:
            confidence, tier, reason = rules.assess_confidence(network, failure_event)
            tier_label = {2: "TIER-2/LLM+VALIDATE", 3: "TIER-3/LLM-EMERGENCY"}.get(tier, "TIER-2/LLM+VALIDATE")
            logger.log(f"[t={env.now:.1f}] [ROUTER] confidence={confidence}% → {tier_label} ({reason})")

            summary, action, target = decide_and_recover(network, failure_event, env.now)

            logger.log(f"\n[t={env.now:.1f}] LLM Decision: {summary}")
            logger.log_llm_decision(env.now, failure_event.failure_type, failure_event.target, summary)

            rule_action, _ = rules.get_rule_suggestion(network, failure_event)
            decision_id = scorer.register_decision(
                current_time=env.now,
                action=action,
                target=target,
                source="llm",
                failure_type=failure_event.failure_type,
                network=network,
                rule_suggestion=rule_action,
            )
            pending_scores[decision_id] = failure_event
            schedule_feedback_check(target, action, env.now, network)

    
    future_time = env.now + 10.0
    ready_ids = list(pending_scores.keys())
    for d_id in ready_ids:
        failure_event = pending_scores.pop(d_id)
        resolved = getattr(failure_event, "resolved", False)
        score = scorer.score_outcome(d_id, future_time, network, resolved=resolved)
        if score:
            logger.log(
                f"[t={future_time:.1f}] [SCORE] {score['target']}: total={score['total_score']:.0f} "
                f"(speed={score['speed_score']:.0f}, stability={score['stability_score']:.0f}, "
                f"cascade={score['cascade_score']:.0f}, delivery={score['delivery_score']:.0f}) "
                f"source={score['source']}"
            )
            
            node = network.nodes.get(score['target'])
            node_role = node.role if node else "unknown"
            concurrent = sum(1 for n in network.nodes.values() if n.status in ("failed", "degraded"))
            health_at = node.health if node else 0
            memory.record_outcome(
                failure_type=score.get('failure_type', 'unknown'),
                node_role=node_role,
                action=score['action'],
                source=score['source'],
                score=score['total_score'],
                resolved=resolved,
                mttr=score.get('mttr'),
                cluster_size=len(network.nodes),
                concurrent_failures=concurrent,
                health_at_decision=health_at,
            )

    
    memory.save()
    mem_stats = memory.get_summary_stats()
    if mem_stats.get("total_entries", 0) > 0:
        logger.log(f"\n[MEMORY] Cross-run learning: {mem_stats['total_entries']} decisions stored")
        if "by_action" in mem_stats:
            for action, stats in mem_stats["by_action"].items():
                logger.log(
                    f"  {action}: {stats['count']}x, avg_score={stats['avg_score']}, "
                    f"resolved={stats['resolved_pct']}%"
                )

    print_simulation_metrics(logger, injector, network)
    print_score_summary(logger, scorer)

    
    features_used = []
    if total_predictive_actions > 0:
        features_used.append(f"Predictive: {total_predictive_actions} preemptive actions on {sorted(predictive_actions_taken)}")
    if total_proactive_actions > 0:
        features_used.append(f"Proactive: {total_proactive_actions} cluster optimizations")
    if total_shed_events > 0 or total_readmit_events > 0:
        from llm_agent import _shed_nodes
        shed_end = sorted(_shed_nodes) if _shed_nodes else "none"
        features_used.append(f"Load shedding: {total_shed_events} shed, {total_readmit_events} readmit, still shed={shed_end}")

    if features_used:
        logger.log_console("\n── Advanced Features ─────────────")
        for feat in features_used:
            logger.log_console(f"  {feat}")

    logger.log_console("\n── LLM Final Analysis ─────────────")
    final_report = analyze_simulation(
        network=network,
        failure_history=injector.history,
        action_history=None,
        scorer_summary=scorer.get_summary(),
    )
    logger.log_console(final_report)

    
    hist = injector.history
    resolved = [f for f in hist if getattr(f, "resolved", False) and getattr(f, "resolved_at", None) is not None]
    active = [f for f in hist if not getattr(f, "resolved", False)]
    logger.log_console(f"\n── Failures: {len(resolved)} resolved / {len(active)} active / {len(hist)} total ──")
    if resolved:
        mttrs = [f.resolved_at - f.time for f in resolved]
        logger.log_console(f"  Avg MTTR: {sum(mttrs)/len(mttrs):.1f} ticks")

    
    logger.log("\n── Detailed Failure History ────────")
    summarize_failures(logger, injector)
    print_final_node_states(logger, network)
    print_final_network_stats(logger, network)

    logger.log(f"\n  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log("")

    log_path = f"simulation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logger.save(log_path)
    logger.log_console(f"\n✓ Full log saved to: {log_path}")
    logger.log_console(" Simulation complete.")

    return {
        "logger": logger,
        "network": network,
        "injector": injector,
        "scorer": scorer,
        "log_path": log_path,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            n = int(sys.argv[1])
        except ValueError:
            print("Usage: python3 main.py <num_nodes>")
            sys.exit(1)
    else:
        n = 5

    run_simulation(n)