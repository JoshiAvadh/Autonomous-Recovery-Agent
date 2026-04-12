
def _is_shed(network, node_id):
    """Check if a node is currently shed for load shedding."""
    if hasattr(network, "is_node_shed"):
        return network.is_node_shed(node_id)
    
    try:
        from llm_agent import _shed_nodes
        return node_id in _shed_nodes
    except ImportError:
        return False


class RuleBasedRecovery:
    
    TIER1_THRESHOLD = 75
    TIER2_THRESHOLD = 45

    def __init__(self):
        self.actions_taken = []
        self.action_count = 0

    def assess_confidence(self, network, failure_event):
        """
        Evaluate how confident the rule engine is about handling this failure.
        Returns (confidence: int 0-100, tier: int 1-3, reason: str)

        v4: Shed nodes are excluded from healthy/degraded/failed counts.
        Intentionally shed nodes should not inflate cascade risk or reduce
        the confidence score — they were taken down on purpose.
        """
        failure_type = failure_event.failure_type
        target = failure_event.target
        node = network.nodes.get(target)

        
        healthy_nodes = [
            n for n in network.nodes.values()
            if n.status == "healthy" and not _is_shed(network, n.id)
        ]
        degraded_nodes = [
            n for n in network.nodes.values()
            if n.status == "degraded" and not _is_shed(network, n.id)
        ]
        failed_nodes = [
            n for n in network.nodes.values()
            if n.status == "failed" and not _is_shed(network, n.id)
        ]

        concurrent_issues = len(degraded_nodes) + len(failed_nodes)
        healthy_count = len(healthy_nodes)
        
        total_nodes = sum(
            1 for n in network.nodes.values()
            if not _is_shed(network, n.id)
        )

        
        if _is_shed(network, target):
            return 100, 1, "target node is shed — no action needed"

        
        base_confidence = {
            "crash": 82,
            "network_partition": 72,
            "latency_spike": 72,
            "memory_leak": 45,
            "memory_leak_early": 55,
            "cpu_overload": 78,
            "predicted_failure": 30,
            "follow_up": 20,
        }.get(failure_type, 50)

        confidence = base_confidence
        reasons = []

        
        if concurrent_issues >= 3:
            confidence -= 35
            reasons.append(f"{concurrent_issues} nodes degraded/failed — high cascade risk")
        elif concurrent_issues >= 2:
            confidence -= 22
            reasons.append(f"{concurrent_issues} nodes degraded/failed — cascade risk")
        elif concurrent_issues == 1 and failure_type in ("latency_spike", "cpu_overload", "network_partition"):
            confidence -= 10
            reasons.append("another active issue already exists")

        
        if healthy_count <= 2:
            confidence -= 25
            reasons.append(f"only {healthy_count} healthy nodes — cluster at risk")
        elif healthy_count <= 3:
            confidence -= 12
            reasons.append(f"only {healthy_count} healthy nodes")

        
        if node and node.role == "Primary":
            healthy_replicas = sum(1 for n in healthy_nodes if n.role == "Replica")
            if healthy_replicas == 0:
                confidence -= 30
                reasons.append("Primary affected with no healthy replicas")
            elif healthy_replicas == 1:
                confidence -= 14
                reasons.append("Primary affected with only 1 healthy replica")

        
        if failure_type == "crash" and node and node.role == "Replica":
            healthy_replicas = sum(1 for n in healthy_nodes if n.role == "Replica")
            if healthy_replicas <= 1:
                confidence -= 12
                reasons.append("Replica affected with little replication slack")

        
        if failure_type in ("memory_leak", "memory_leak_early") and node:
            if 15 <= node.health <= 60:
                confidence -= 18
                reasons.append(f"health={node.health:.0f}% — ambiguous restart vs isolate zone")
            elif node.health > 60:
                confidence -= 8
                reasons.append("memory leak needs judgement, not pure rules")

        
        if failure_type == "latency_spike":
            avg_load = (
                sum(n.load for n in healthy_nodes) / len(healthy_nodes)
                if healthy_nodes else 100
            )
            if avg_load > 55:
                confidence -= 18
                reasons.append(f"avg load={avg_load:.0f}% — cluster under stress")
            if avg_load > 70:
                confidence -= 10
                reasons.append("high average load during latency spike")

        
        if failure_type == "cpu_overload" and node:
            if node.load >= 85:
                confidence -= 10
                reasons.append(f"load={node.load:.0f}% — severe overload")
            if concurrent_issues >= 1:
                confidence -= 10
                reasons.append("overload during active recovery")

        
        if failure_type == "network_partition":
            if "<->" in target:
                try:
                    a, b = target.split("<->")
                    na = network.nodes.get(a)
                    nb = network.nodes.get(b)
                    if na and nb and (na.role in ("Primary", "Replica") or nb.role in ("Primary", "Replica")):
                        confidence -= 12
                        reasons.append("partition affects consensus path")
                except Exception:
                    pass

        
        if concurrent_issues == 0 and healthy_count >= total_nodes - 1:
            confidence += 8
            reasons.append("cluster is healthy — straightforward recovery")

        confidence = max(0, min(100, confidence))

        if confidence >= self.TIER1_THRESHOLD:
            tier = 1
        elif confidence >= self.TIER2_THRESHOLD:
            tier = 2
        else:
            tier = 3

        reason_str = "; ".join(reasons) if reasons else "standard case"
        return confidence, tier, reason_str

    def decide(self, network, failure_event, current_time=0):
        """Execute a rule-based decision. Returns (decision_str, action).

        v4: All load-distribution actions (rebalance_load, reroute_traffic)
        now exclude shed nodes from their target sets.
        """
        failure_type = failure_event.failure_type
        target = failure_event.target
        node = network.nodes.get(target)

        
        if _is_shed(network, target):
            decision = f"[RULE] Action: monitor_only on {target} → Result: Node is shed — no action"
            self.actions_taken.append({
                "time": current_time,
                "action": "monitor_only",
                "target": target,
                "result": "Node is shed — no action",
            })
            self.action_count += 1
            return decision, "monitor_only"

        action = "monitor_only"
        result = "No rule matched"

        if failure_type == "crash":
            if node and node.role == "Primary":
                if hasattr(network, "elect_new_primary"):
                    new_primary = network.elect_new_primary()
                    if new_primary:
                        action = "promote_replica"
                        result = f"Promoted {new_primary} to Primary"
                    else:
                        action = "restart_node"
                        result = f"No healthy replica found, restarted {target}"
                else:
                    for nid, n in network.nodes.items():
                        if n.role == "Replica" and n.status == "healthy":
                            n.role = "Primary"
                            n.status = "healthy"
                            n.health = min(100, n.health + 20)
                            action = "promote_replica"
                            result = f"Promoted {nid} to Primary"
                            break
                    else:
                        result = "No healthy replica found"
            else:
                if node:
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
                        node.health = 70.0
                        node.load = 20.0
                    action = "restart_node"
                    result = f"Restarted {target}"

        elif failure_type == "network_partition":
           
            healthy = [
                n for n in network.nodes.values()
                if n.status == "healthy" and not _is_shed(network, n.id)
            ]
            if healthy:
                total = sum(n.load for n in healthy)
                avg = total / len(healthy)
                for n in healthy:
                    n.load = avg
                action = "rebalance_load"
                result = f"Rebalanced to {avg:.0f}%"

        elif failure_type == "memory_leak":
            
            if node:
                node.status = "failed"
                node.health = 0.0
                node.load = 0.0
                for other_id in network.nodes:
                    if other_id != target:
                        network.partition(target, other_id)
                action = "isolate_node"
                result = f"Isolated {target}"

        elif failure_type == "memory_leak_early":
            
            if node:
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
                action = "restart_node"
                result = f"Restarted {target}"

        elif failure_type == "latency_spike":
            
            healthy = [
                n for n in network.nodes.values()
                if n.status == "healthy" and not _is_shed(network, n.id)
            ]
            if healthy:
                avg_load = sum(n.load for n in healthy) / len(healthy)
                hottest = max(healthy, key=lambda n: n.load)

                
                if avg_load > 65 or hottest.load > 80:
                    excess = hottest.load * 0.5
                    hottest.load = hottest.load * 0.5
                    share_to = [n for n in healthy if n.id != hottest.id]
                    if share_to:
                        per_node = excess / len(share_to)
                        for n in share_to:
                            n.load = min(100, n.load + per_node)
                    action = "reroute_traffic"
                    result = f"Rerouted load from {hottest.id}"
                else:
                    total = sum(n.load for n in healthy)
                    avg = total / len(healthy)
                    for n in healthy:
                        n.load = avg
                    action = "rebalance_load"
                    result = f"Rebalanced to {avg:.0f}%"

        elif failure_type == "cpu_overload":
            if node:
                node.load = max(10, node.load * 0.7)
                action = "reroute_traffic"
                result = f"Reduced {target} load to {node.load:.0f}%"

        decision = f"[RULE] Action: {action} on {target} → Result: {result}"
        self.actions_taken.append({
            "time": current_time,
            "action": action,
            "target": target,
            "result": result,
        })
        self.action_count += 1
        return decision, action

    def get_rule_suggestion(self, network, failure_event):
        """
        Return what the rule engine WOULD do, without executing it.
        Used for comparison when LLM makes the actual decision.

        v4: Shed-aware — excludes shed nodes from target selection.
        """
        failure_type = failure_event.failure_type
        target = failure_event.target
        node = network.nodes.get(target)

        
        if _is_shed(network, target):
            return "monitor_only", target

        if failure_type == "crash":
            if node and node.role == "Primary":
                for nid, n in network.nodes.items():
                    if n.role == "Replica" and n.status == "healthy":
                        return "promote_replica", nid
                return "restart_node", target
            return "restart_node", target

        elif failure_type == "network_partition":
            return "rebalance_load", target

        elif failure_type == "memory_leak":
            return "isolate_node", target

        elif failure_type == "memory_leak_early":
            return "restart_node", target

        elif failure_type == "latency_spike":
            
            healthy = [
                n for n in network.nodes.values()
                if n.status == "healthy" and not _is_shed(network, n.id)
            ]
            if healthy:
                avg_load = sum(n.load for n in healthy) / len(healthy)
                hottest = max(healthy, key=lambda n: n.load)
                if avg_load > 65 or hottest.load > 80:
                    return "reroute_traffic", hottest.id
            return "rebalance_load", target

        elif failure_type == "cpu_overload":
            return "reroute_traffic", target

        return "monitor_only", target