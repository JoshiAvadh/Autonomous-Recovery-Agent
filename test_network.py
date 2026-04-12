# test_network.py
# Tests the SimPy-based distributed network simulation
import simpy
from network import build_cluster, HEALTHY, DEGRADED, FAILED


def test_cluster_creation():
    """Test that clusters are created with correct roles."""
    print("\n== Test: Cluster Creation ==")
    env = simpy.Environment()
    config = [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
        {"id": "N4", "role": "Worker"},
        {"id": "N5", "role": "Gateway"},
    ]
    network = build_cluster(env, config)

    assert len(network.nodes) == 5, f"Expected 5 nodes, got {len(network.nodes)}"
    assert network.current_primary == "N1", f"Expected N1 as primary, got {network.current_primary}"
    assert network.nodes["N2"].role == "Replica"
    assert network.nodes["N4"].role == "Worker"
    print(f"  Created {len(network.nodes)} nodes, primary={network.current_primary}")
    print("  PASSED")


def test_single_primary_invariant():
    """Test that only one primary exists even if config has two."""
    print("\n== Test: Single Primary Invariant ==")
    env = simpy.Environment()
    config = [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Primary"},  # Should be demoted to Replica
        {"id": "N3", "role": "Replica"},
    ]
    network = build_cluster(env, config)

    primaries = [n for n in network.nodes.values() if n.role == "Primary"]
    assert len(primaries) == 1, f"Expected 1 primary, got {len(primaries)}"
    print(f"  Primaries: {[n.id for n in primaries]}")
    print("  PASSED")


def test_partition_and_heal():
    """Test network partition creation and healing."""
    print("\n== Test: Partition and Heal ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
    ])

    network.partition("N1", "N2")
    assert ("N1", "N2") in network.partitions, "Partition should exist"
    print(f"  Partitions after cut: {network.partitions}")

    network.heal_partition("N1", "N2")
    assert ("N1", "N2") not in network.partitions, "Partition should be healed"
    print(f"  Partitions after heal: {network.partitions}")
    print("  PASSED")


def test_quorum_check():
    """Test quorum availability detection."""
    print("\n== Test: Quorum Check ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
    ])
    env.run(until=0.1)

    assert network._has_quorum(), "Should have quorum with all nodes healthy"
    print(f"  Quorum with all healthy: {network._has_quorum()}")

    # Fail 2 of 3 — should lose quorum
    network.nodes["N2"].status = FAILED
    network.nodes["N3"].status = FAILED
    has_q = network._has_quorum()
    print(f"  Quorum with 2 failed: {has_q}")
    # With 3 voters, need 2 — only N1 is up, so no quorum
    assert not has_q, "Should NOT have quorum with 2 of 3 nodes failed"
    print("  PASSED")


def test_leader_election():
    """Test leader election after primary failure."""
    print("\n== Test: Leader Election ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
    ])
    env.run(until=0.1)

    # Fail primary
    network.nodes["N1"].status = FAILED
    old_primary = network.current_primary
    new_primary = network.elect_new_primary()
    print(f"  Old primary: {old_primary}, new primary: {new_primary}")
    assert new_primary is not None, "Should elect a new primary"
    assert new_primary != "N1", "Failed node should not be elected"
    assert network.metrics["leader_elections"] >= 1, "Should count the election"
    print("  PASSED")


def test_rejoin_node():
    """Test node rejoin after failure."""
    print("\n== Test: Node Rejoin ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=0.1)

    # Fail and rejoin N3
    network.nodes["N3"].status = FAILED
    network.nodes["N3"].health = 0
    rejoined = network.rejoin_node("N3", preferred_role="Worker", degraded=True, health=40.0, load=20.0)

    assert rejoined is not None, "Should return rejoined node"
    assert rejoined.status == DEGRADED, f"Should be degraded, got {rejoined.status}"
    assert rejoined.health == 40.0, f"Health should be 40, got {rejoined.health}"
    print(f"  Rejoined N3: status={rejoined.status}, health={rejoined.health}")
    print("  PASSED")


def test_simulation_runs():
    """Test that simulation runs for expected duration with message delivery."""
    print("\n== Test: Full Simulation Run ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=10)

    stats = network.get_stats()
    print(f"  Sim time: {env.now}")
    print(f"  Messages: {stats['total_messages']} total, {stats['delivered']} delivered")
    print(f"  Requests: {stats['requests_total']} total, {stats['requests_completed']} completed")
    assert env.now >= 10, "Simulation should reach t=10"
    assert stats["total_messages"] > 0, "Should have generated messages"
    print("  PASSED")


def test_stats_reporting():
    """Test that network stats are computed correctly."""
    print("\n== Test: Stats Reporting ==")
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    env.run(until=5)

    stats = network.get_stats()
    required_keys = [
        "total_messages", "delivered", "dropped", "success_rate",
        "requests_total", "request_timeouts", "timeout_rate",
        "writes_attempted", "writes_committed", "commit_rate",
        "reads_completed", "stale_reads", "stale_read_rate",
        "leader_elections", "current_primary", "commit_index",
    ]
    for key in required_keys:
        assert key in stats, f"Missing stat: {key}"
    print(f"  All {len(required_keys)} required stats present")
    print(f"  Delivery rate: {stats['success_rate']}")
    print("  PASSED")


if __name__ == "__main__":
    print("=" * 60)
    print("  NETWORK SIMULATION TEST SUITE")
    print("=" * 60)

    test_cluster_creation()
    test_single_primary_invariant()
    test_partition_and_heal()
    test_quorum_check()
    test_leader_election()
    test_rejoin_node()
    test_simulation_runs()
    test_stats_reporting()

    print("\n" + "=" * 60)
    print("  ALL TESTS PASSED")
    print("=" * 60)
