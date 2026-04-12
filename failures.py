import simpy
import random
from network import HEALTHY, DEGRADED, FAILED
from event_bus import bus

CRASH              = "crash"
NETWORK_PARTITION  = "network_partition"
LATENCY_SPIKE      = "latency_spike"
MEMORY_LEAK        = "memory_leak"
CPU_OVERLOAD       = "cpu_overload"

ALL_FAILURE_TYPES = [CRASH, NETWORK_PARTITION, LATENCY_SPIKE, MEMORY_LEAK, CPU_OVERLOAD]

_HEALTH_RESTORE_INTERVAL = 1.0
_HEALTH_RESTORE_STEP     = 5.0
_HEALTH_RESTORE_TARGET   = 100.0


class FailureEvent:
    def __init__(self, failure_type, target, time, details=""):
        self.failure_type = failure_type
        self.target       = target
        self.time         = time
        self.details      = details
        self.resolved     = False
        self.resolved_at  = None

    def resolve(self, resolved_at):
        self.resolved    = True
        self.resolved_at = resolved_at

    def __repr__(self):
        status = f"resolved@t={self.resolved_at:.1f}" if self.resolved else "ACTIVE"
        return f"[t={self.time:.1f}] {self.failure_type.upper()} on {self.target} ({status})"


class FaultInjector:
    def __init__(self, env, network):
        self.env     = env
        self.network = network
        self.history = []

    def schedule_crash(self, node_id, at_time, recover_after=None):
        self.env.process(self._crash_process(node_id, at_time, recover_after))

    def schedule_partition(self, node_a, node_b, at_time, duration=5.0):
        self.env.process(self._partition_process(node_a, node_b, at_time, duration))

    def schedule_latency_spike(self, at_time, multiplier=5.0, duration=3.0):
        self.env.process(self._latency_spike_process(at_time, multiplier, duration))

    def schedule_memory_leak(self, node_id, at_time, drain_rate=5):
        self.env.process(self._memory_leak_process(node_id, at_time, drain_rate))

    def schedule_cpu_overload(self, node_id, at_time, load_spike=40, duration=6.0):
        self.env.process(self._cpu_overload_process(node_id, at_time, load_spike, duration))

    def schedule_random_failures(self, count=3, between=(5, 50)):
        for _ in range(count):
            at_time      = random.uniform(*between)
            failure_type = random.choice(ALL_FAILURE_TYPES)
            node_ids     = list(self.network.nodes.keys())

            if failure_type == CRASH:
                target = random.choice(node_ids)
                self.schedule_crash(target, at_time, recover_after=random.uniform(3, 8))
            elif failure_type == NETWORK_PARTITION:
                if len(node_ids) >= 2:
                    a, b = random.sample(node_ids, 2)
                    self.schedule_partition(a, b, at_time, duration=random.uniform(3, 7))
            elif failure_type == LATENCY_SPIKE:
                self.schedule_latency_spike(at_time, multiplier=random.uniform(3, 8), duration=random.uniform(2, 5))
            elif failure_type == MEMORY_LEAK:
                target = random.choice(node_ids)
                self.schedule_memory_leak(target, at_time, drain_rate=random.randint(3, 8))
            elif failure_type == CPU_OVERLOAD:
                target = random.choice(node_ids)
                self.schedule_cpu_overload(target, at_time, load_spike=random.randint(30, 60), duration=random.uniform(3, 7))

   

    def _crash_process(self, node_id, at_time, recover_after):
        yield self.env.timeout(at_time)
        node = self.network.nodes.get(node_id)
        if not node or node.status == FAILED:
            return

        original_role = node.role
        was_primary = (getattr(self.network, "current_primary", None) == node_id)

        node.status = FAILED
        node.health = 0
        node.load = 0

        event = FailureEvent(CRASH, node_id, self.env.now, f"{original_role} hard crash")
        self.history.append(event)
        bus.publish(event)
        print(f"[t={self.env.now:.1f}] CRASH: {node_id} ({original_role}) went down")

        if was_primary:
            self.network._last_primary_failure_at = self.env.now

        if recover_after is not None:
            yield self.env.timeout(recover_after)

            if node.status != FAILED:
                if not event.resolved:
                    event.resolve(self.env.now)
                pass  
                return

        
        preferred_role = original_role
        if original_role == "Primary":
            current_primary = getattr(self.network, "current_primary", None)
            if current_primary is not None and current_primary != node_id:
                preferred_role = "Replica"

        if hasattr(self.network, "rejoin_node"):
            self.network.rejoin_node(
                node_id,
                preferred_role=preferred_role,
                degraded=True,
                health=40.0,
                load=20.0,
            )
        else:
            node.role = preferred_role
            node.status = DEGRADED
            node.health = 40
            node.load = 20

        event.resolve(self.env.now)
        pass  

        yield self.env.timeout(2.0)

        if node.status == DEGRADED:
            node.status = HEALTHY
            node.health = 85
            pass  
            self.env.process(self._health_restore_process(node_id, node))

    def _health_restore_process(self, node_id, node):
        """
        IMPROVEMENT: Tick node health back to 100% post-crash.
        Ensures recovered nodes (N1, N3) don't linger at 85%.
        """
        while node.health < _HEALTH_RESTORE_TARGET and node.status == HEALTHY:
            yield self.env.timeout(_HEALTH_RESTORE_INTERVAL)
            if node.status != HEALTHY:
                break
            node.health = min(_HEALTH_RESTORE_TARGET, node.health + _HEALTH_RESTORE_STEP)
            if node.health >= _HEALTH_RESTORE_TARGET:
                pass  

    def _partition_process(self, node_a, node_b, at_time, duration):
        yield self.env.timeout(at_time)
        self.network.partition(node_a, node_b)
        event = FailureEvent(NETWORK_PARTITION, f"{node_a}<->{node_b}", self.env.now)
        self.history.append(event)
        bus.publish(event)
        print(f"[t={self.env.now:.1f}] PARTITION: link {node_a}<->{node_b} cut")

        yield self.env.timeout(duration)
        self.network.heal_partition(node_a, node_b)
        event.resolve(self.env.now)
        pass  

    def _latency_spike_process(self, at_time, multiplier, duration):
        yield self.env.timeout(at_time)
        original = self.network.base_latency
        self.network.base_latency     = original * multiplier
        self.network.spike_active     = True
        self.network.spike_multiplier = multiplier

        event = FailureEvent(LATENCY_SPIKE, "network", self.env.now, f"{multiplier:.1f}x latency")
        self.history.append(event)
        bus.publish(event)
        print(f"[t={self.env.now:.1f}] LATENCY SPIKE: {multiplier:.1f}x slowdown")

        yield self.env.timeout(duration)
        self.network.base_latency     = original
        self.network.spike_active     = False
        self.network.spike_multiplier = 1.0
        event.resolve(self.env.now)
        pass  

    def _memory_leak_process(self, node_id, at_time, drain_rate):
        yield self.env.timeout(at_time)
        node = self.network.nodes.get(node_id)
        if not node:
            return

        event = FailureEvent(MEMORY_LEAK, node_id, self.env.now)
        self.history.append(event)
        bus.publish(event)
        print(f"[t={self.env.now:.1f}] MEMORY LEAK: {node_id} started leaking")

        early_intervention_fired = False

        while node.health > 0 and not event.resolved:
            yield self.env.timeout(1.0)
            node.health = max(0, node.health - drain_rate)

            if node.health <= 50 and node.status == HEALTHY:
                node.status = DEGRADED
                pass  

            
            if (
                not early_intervention_fired
                and node.health <= 50
                and node.health > 20
                and node.status == DEGRADED
                and not event.resolved
            ):
                early_intervention_fired = True
                early_event = FailureEvent(
                    "memory_leak_early",
                    node_id,
                    self.env.now,
                    f"Early intervention at health={node.health:.0f}%"
                )
                
                early_event._parent_event = event
                bus.publish(early_event)
                pass  

            if node.health <= 0:
                node.status = FAILED
                node.load   = 0
                pass  
                break

    def _cpu_overload_process(self, node_id, at_time, load_spike, duration):
        yield self.env.timeout(at_time)
        node = self.network.nodes.get(node_id)
        if not node or node.status == FAILED:
            return

        original  = node.load
        node.load = min(100, node.load + load_spike)
        node.status = DEGRADED
        event = FailureEvent(CPU_OVERLOAD, node_id, self.env.now, f"load spiked to {node.load:.0f}%")
        self.history.append(event)
        bus.publish(event)
        print(f"[t={self.env.now:.1f}] CPU OVERLOAD: {node_id} at {node.load:.0f}% load")

        yield self.env.timeout(duration)
        node.load   = max(original, 20)
        node.status = HEALTHY
        event.resolve(self.env.now)
        pass  

    def get_active_failures(self):
        return [e for e in self.history if not e.resolved]

    def summary(self):
        print("\n── Failure History ──────────────────────────────────────")
        if not self.history:
            print("  No failures recorded.")
            return
        for ev in self.history:
            print(f"  {ev}")
        active = self.get_active_failures()
        print(f"\n  Total: {len(self.history)} | Active: {len(active)} | Resolved: {len(self.history)-len(active)}")
