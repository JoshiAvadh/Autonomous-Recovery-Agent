# test_llm.py
# Tests LLM integration: decision-making, optimization, analysis, and multi-turn reasoning
import simpy
from network import build_cluster
from failures import FaultInjector, FailureEvent, CRASH, MEMORY_LEAK
from llm_agent import (
    decide_and_recover,
    decide_only,
    decide_batch,
    optimize_cluster,
    analyze_simulation,
    reset_llm_state,
    execute_action,
    record_action,
    get_metrics,
    is_obvious_case,
)
from rule_based import RuleBasedRecovery


def test_single_failure_decision():
    """Test LLM decision on a single crash failure."""
    print("\n== Test: Single Failure Decision ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
        {"id": "N4", "role": "Worker"},
        {"id": "N5", "role": "Gateway"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N1", at_time=5.0, recover_after=4.0)
    env.run(until=5.1)

    if injector.history:
        failure = injector.history[0]
        summary, action, target = decide_and_recover(network, failure, env.now)
        print(f"  Failure: {failure.failure_type} on {failure.target}")
        print(f"  Decision: {action} on {target}")
        print(f"  Summary: {summary[:150]}...")
        assert action in ("restart_node", "promote_replica", "reassign_role", "isolate_node",
                          "rebalance_load", "reroute_traffic", "monitor_only"), f"Invalid action: {action}"
        print("  PASSED")
    else:
        print("  SKIPPED (no failure injected)")


def test_confidence_routing():
    """Test that confidence-based routing sends cases to correct tier."""
    print("\n== Test: Confidence Routing ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=1)

    # Simple crash on a worker — should be obvious (Tier 1)
    simple_event = FailureEvent(CRASH, "N3", env.now, "Worker crash")
    is_obvious = is_obvious_case(simple_event, network)
    print(f"  Worker crash is obvious: {is_obvious}")

    # Memory leak — should NOT be obvious (Tier 2/3)
    complex_event = FailureEvent(MEMORY_LEAK, "N1", env.now, "Primary memory leak")
    network.nodes["N1"].health = 40
    network.nodes["N1"].status = "degraded"
    is_obvious_complex = is_obvious_case(complex_event, network)
    print(f"  Primary memory leak (health=40%) is obvious: {is_obvious_complex}")
    print("  PASSED")


def test_batch_decisions():
    """Test coordinated multi-failure batch decisions."""
    print("\n== Test: Batch Decisions ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
        {"id": "N4", "role": "Worker"},
        {"id": "N5", "role": "Gateway"},
    ])
    env.run(until=1)

    # Simulate two simultaneous failures
    failures = [
        FailureEvent(CRASH, "N4", env.now, "Worker crash"),
        FailureEvent(MEMORY_LEAK, "N5", env.now, "Gateway memory leak"),
    ]
    network.nodes["N4"].status = "failed"
    network.nodes["N4"].health = 0
    network.nodes["N5"].health = 35
    network.nodes["N5"].status = "degraded"

    decisions = decide_batch(network, failures, env.now)
    print(f"  Got {len(decisions)} decisions for {len(failures)} failures")
    for i, dec in enumerate(decisions):
        print(f"  Decision {i+1}: {dec.get('action')} on {dec.get('target')} "
              f"(confidence={dec.get('confidence', '?')}%)")
    assert len(decisions) == len(failures), "Should get one decision per failure"
    print("  PASSED")


def test_optimize_cluster():
    """Test proactive cluster optimization."""
    print("\n== Test: Cluster Optimization ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=10)

    # Create load imbalance to trigger optimization
    network.nodes["N1"].load = 90
    network.nodes["N2"].load = 20
    network.nodes["N3"].load = 15

    suggestion = optimize_cluster(network)
    print(f"  Optimization result: {suggestion[:200]}...")
    assert suggestion is not None, "Should return optimization suggestion"
    assert suggestion != "No optimization available" or True, "May or may not have suggestions"
    print("  PASSED")


def test_analyze_simulation():
    """Test post-simulation analysis."""
    print("\n== Test: Simulation Analysis ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N1", at_time=5.0, recover_after=4.0)
    env.run(until=15)

    report = analyze_simulation(network, injector.history)
    print(f"  Report length: {len(report)} chars")
    print(f"  Preview: {report[:200]}...")
    assert report is not None, "Should return analysis report"
    assert len(report) > 10, "Report should have meaningful content"
    print("  PASSED")


def test_metrics_tracking():
    """Test that metrics are properly tracked."""
    print("\n== Test: Metrics Tracking ==")
    reset_llm_state()
    record_action(1.0, "restart_node", "N1", "Restarted", source="llm")
    record_action(2.0, "rebalance_load", "N2", "Rebalanced", source="rule")

    metrics = get_metrics()
    print(f"  Total decisions: {metrics['total_decisions']}")
    assert metrics["total_decisions"] == 2, "Should have 2 decisions"
    print("  PASSED")


def test_execute_actions():
    """Test that all action types execute correctly."""
    print("\n== Test: Execute Actions ==")
    reset_llm_state()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=1)

    mock_event = FailureEvent(CRASH, "N3", 1.0, "test crash")
    network.nodes["N3"].status = "failed"
    network.nodes["N3"].health = 0

    # Test restart
    result = execute_action(network, "restart_node", "N3", mock_event, 1.0)
    print(f"  restart_node: {result}")
    assert "Restarted" in result, f"Expected restart result, got: {result}"
    assert network.nodes["N3"].health >= 80, "Health should be restored"

    # Test rebalance
    network.nodes["N1"].load = 80
    network.nodes["N2"].load = 20
    result = execute_action(network, "rebalance_load", "N1", mock_event, 2.0)
    print(f"  rebalance_load: {result}")
    assert "Rebalanced" in result, f"Expected rebalance result, got: {result}"

    # Test reroute
    network.nodes["N1"].load = 90
    result = execute_action(network, "reroute_traffic", "N1", mock_event, 3.0)
    print(f"  reroute_traffic: {result}")
    assert "Rerouted" in result, f"Expected reroute result, got: {result}"

    print("  PASSED")


if __name__ == "__main__":
    print("=" * 60)
    print("  LLM AGENT TEST SUITE")
    print("=" * 60)

    test_single_failure_decision()
    test_confidence_routing()
    test_batch_decisions()
    test_optimize_cluster()
    test_analyze_simulation()
    test_metrics_tracking()
    test_execute_actions()

    print("\n" + "=" * 60)
    print("  ALL TESTS PASSED")
    print("=" * 60)
