def generate_cluster(size=5):
    """
    Generate a cluster config of given size.
    Roles are assigned as:
    - 1 Primary
    - 2 Replicas
    - Rest alternating between Worker and Gateway
    """
    config = []
    for i in range(1, size + 1):
        node_id = f"N{i}"
        if i == 1:
            role = "Primary"
        elif i <= 3:
            role = "Replica"
        elif i % 2 == 0:
            role = "Worker"
        else:
            role = "Gateway"
        config.append({"id": node_id, "role": role})
    return config


def generate_failures(injector, cluster_size=5):
    """
    Schedule failures scaled to cluster size.
    Includes conflict scenarios, ambiguous cascades, and situations
    that REQUIRE LLM reasoning — rules alone cannot resolve them optimally.

    Key design principles:
    - Phase 1-2: Warmup (rules handle), establishes baseline
    - Phase 3-4: Overlapping failures that drop rule confidence to Tier 2/3
    - Phase 5-6: Ambiguous cascades where the optimal action depends on
      understanding trade-offs (restart vs isolate, quorum vs throughput)
    - Phase 7: Recovery stress — optimization needed during recovery window
    """
    
    injector.schedule_crash("N1", at_time=10, recover_after=5)

    
    injector.schedule_partition("N2", "N3", at_time=25, duration=4.0)
    injector.schedule_memory_leak("N4", at_time=26, drain_rate=3)

   
    injector.schedule_latency_spike(at_time=30.0, multiplier=2.0, duration=3.0)

    
    injector.schedule_latency_spike(at_time=40.0, multiplier=1.5, duration=3.0)

    injector.schedule_crash("N3", at_time=51, recover_after=5)

    
    injector.schedule_cpu_overload("N5", at_time=54, load_spike=45, duration=5.0)

    
    if cluster_size >= 5:
        injector.schedule_latency_spike(at_time=62.0, multiplier=3.0, duration=4.0)
        injector.schedule_cpu_overload("N2", at_time=63, load_spike=50, duration=5.0)

        
        injector.schedule_memory_leak("N1", at_time=44, drain_rate=2)
        injector.schedule_partition("N1", "N2", at_time=45, duration=3.0)

    
    if cluster_size >= 10:
        injector.schedule_crash("N6", at_time=15, recover_after=4)
        injector.schedule_memory_leak("N7", at_time=35)
        injector.schedule_cpu_overload("N8", at_time=45, load_spike=40, duration=6)

        
        injector.schedule_crash("N9", at_time=42, recover_after=6)
        injector.schedule_partition("N6", "N8", at_time=42, duration=5)

        
        injector.schedule_cpu_overload("N10", at_time=55, load_spike=55, duration=4)
        injector.schedule_memory_leak("N6", at_time=56, drain_rate=4)

    if cluster_size >= 20:
        injector.schedule_partition("N10", "N11", at_time=20, duration=5)
        injector.schedule_crash("N15", at_time=38, recover_after=6)
        injector.schedule_latency_spike(at_time=48.0, multiplier=2.5, duration=4.0)
        injector.schedule_random_failures(count=3, between=(5, 55))

        
        injector.schedule_memory_leak("N12", at_time=30, drain_rate=4)
        injector.schedule_memory_leak("N14", at_time=32, drain_rate=5)

        
        injector.schedule_crash("N16", at_time=58, recover_after=5)
        injector.schedule_cpu_overload("N17", at_time=59, load_spike=60, duration=4)
        injector.schedule_partition("N18", "N19", at_time=59, duration=4)
        injector.schedule_memory_leak("N20", at_time=60, drain_rate=6)

        
        injector.schedule_cpu_overload("N1", at_time=35, load_spike=50, duration=5)
        injector.schedule_memory_leak("N2", at_time=36, drain_rate=3)
