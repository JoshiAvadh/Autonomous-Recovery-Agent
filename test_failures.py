# test_failures.py
# Tests fault injection, failure resolution, and recovery mechanics
import simpy
from network import build_cluster, HEALTHY, DEGRADED, FAILED
from failures import FaultInjector, FailureEvent
from event_bus import bus


def test_crash_injection():
    """Test that crash injection fails node and recovers it."""
    print("\n== Test: Crash Injection ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N3", at_time=2.0, recover_after=3.0)

    env.run(until=2.5)
    assert network.nodes["N3"].status == FAILED, f"N3 should be failed, got {network.nodes['N3'].status}"
    assert network.nodes["N3"].health == 0, "N3 health should be 0"
    print(f"  At t=2.5: N3 status={network.nodes['N3'].status}, health={network.nodes['N3'].health}")

    env.run(until=8.0)
    assert network.nodes["N3"].status != FAILED, f"N3 should have recovered, got {network.nodes['N3'].status}"
    print(f"  At t=8.0: N3 status={network.nodes['N3'].status}, health={network.nodes['N3'].health}")
    print("  PASSED")


def test_partition_injection():
    """Test network partition creation and auto-healing."""
    print("\n== Test: Partition Injection ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_partition("N1", "N2", at_time=2.0, duration=3.0)

    env.run(until=2.5)
    assert ("N1", "N2") in network.partitions, "Partition should be active"
    print(f"  At t=2.5: partitions={network.partitions}")

    env.run(until=6.0)
    assert ("N1", "N2") not in network.partitions, "Partition should be healed"
    print(f"  At t=6.0: partitions={network.partitions}")
    print("  PASSED")


def test_latency_spike():
    """Test latency spike increases and restores base latency."""
    print("\n== Test: Latency Spike ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
    ])
    injector = FaultInjector(env, network)
    original_latency = network.base_latency
    injector.schedule_latency_spike(at_time=2.0, multiplier=4.0, duration=2.0)

    env.run(until=2.5)
    assert network.base_latency > original_latency, "Latency should be elevated"
    assert network.spike_active, "Spike should be active"
    print(f"  At t=2.5: latency={network.base_latency:.2f} (was {original_latency:.2f})")

    env.run(until=5.0)
    assert abs(network.base_latency - original_latency) < 0.01, "Latency should be restored"
    assert not network.spike_active, "Spike should be inactive"
    print(f"  At t=5.0: latency={network.base_latency:.2f} (restored)")
    print("  PASSED")


def test_memory_leak():
    """Test that memory leak gradually drains health."""
    print("\n== Test: Memory Leak ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_memory_leak("N3", at_time=1.0, drain_rate=10)

    env.run(until=1.5)
    initial_health = network.nodes["N3"].health
    print(f"  At t=1.5: N3 health={initial_health:.0f}%")

    env.run(until=5.0)
    later_health = network.nodes["N3"].health
    print(f"  At t=5.0: N3 health={later_health:.0f}%")
    assert later_health < initial_health, "Health should have decreased"
    print("  PASSED")


def test_cpu_overload():
    """Test CPU overload spike and recovery."""
    print("\n== Test: CPU Overload ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_cpu_overload("N3", at_time=2.0, load_spike=50, duration=3.0)

    env.run(until=2.5)
    assert network.nodes["N3"].status == DEGRADED, f"N3 should be degraded, got {network.nodes['N3'].status}"
    print(f"  At t=2.5: N3 load={network.nodes['N3'].load:.0f}%, status={network.nodes['N3'].status}")

    env.run(until=6.0)
    assert network.nodes["N3"].status == HEALTHY, f"N3 should have recovered, got {network.nodes['N3'].status}"
    print(f"  At t=6.0: N3 load={network.nodes['N3'].load:.0f}%, status={network.nodes['N3'].status}")
    print("  PASSED")


def test_failure_history():
    """Test that failure history is properly maintained."""
    print("\n== Test: Failure History ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Worker"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N3", at_time=2.0, recover_after=3.0)
    injector.schedule_partition("N1", "N2", at_time=4.0, duration=2.0)
    injector.schedule_latency_spike(at_time=6.0, multiplier=3.0, duration=2.0)

    env.run(until=12.0)

    print(f"  Total failures recorded: {len(injector.history)}")
    resolved = [f for f in injector.history if f.resolved]
    active = injector.get_active_failures()
    print(f"  Resolved: {len(resolved)}, Active: {len(active)}")

    assert len(injector.history) == 3, f"Expected 3 failures, got {len(injector.history)}"
    assert len(resolved) >= 2, f"At least 2 should be resolved, got {len(resolved)}"

    for f in injector.history:
        print(f"    {f}")
    print("  PASSED")


def test_event_bus_integration():
    """Test that failures publish events to the event bus."""
    print("\n== Test: Event Bus Integration ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N2", at_time=1.0, recover_after=2.0)

    env.run(until=1.5)

    assert bus.has_events(), "Event bus should have events from the crash"
    event = bus.consume(timeout=0)
    assert event is not None, "Should consume an event"
    assert event.failure_type == "crash", f"Expected crash event, got {event.failure_type}"
    assert event.target == "N2", f"Expected N2 target, got {event.target}"
    print(f"  Consumed event: {event.failure_type} on {event.target}")
    print("  PASSED")


def test_primary_crash_election():
    """Test that primary crash triggers leader election via consensus loop."""
    print("\n== Test: Primary Crash Election ==")
    bus.reset()
    env = simpy.Environment()
    network = build_cluster(env, [
        {"id": "N1", "role": "Primary"},
        {"id": "N2", "role": "Replica"},
        {"id": "N3", "role": "Replica"},
    ])
    injector = FaultInjector(env, network)
    injector.schedule_crash("N1", at_time=2.0, recover_after=10.0)

    env.run(until=6.0)  # Allow time for consensus loop to detect and elect

    assert network.current_primary != "N1" or network.nodes["N1"].status != FAILED, \
        "Should have elected a new primary or N1 recovered"
    elections = network.metrics["leader_elections"]
    print(f"  Primary after crash: {network.current_primary}")
    print(f"  Leader elections: {elections}")
    assert elections >= 1, "Should have triggered at least one election"
    print("  PASSED")


if __name__ == "__main__":
    print("=" * 60)
    print("  FAILURE INJECTION TEST SUITE")
    print("=" * 60)

    test_crash_injection()
    test_partition_injection()
    test_latency_spike()
    test_memory_leak()
    test_cpu_overload()
    test_failure_history()
    test_event_bus_integration()
    test_primary_crash_election()

    print("\n" + "=" * 60)
    print("  ALL TESTS PASSED")
    print("=" * 60)
