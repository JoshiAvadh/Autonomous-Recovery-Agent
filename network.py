import random
import simpy

HEALTHY = "healthy"
DEGRADED = "degraded"
FAILED = "failed"

MAX_RETRIES = 2
RETRY_BASE_DELAY = 0.5
RETRY_BACKOFF_MULT = 2.0
BASE_PACKET_LOSS = 0.01

DEFAULT_REQUEST_TIMEOUT = 12.0
HEARTBEAT_INTERVAL = (1.2, 2.8)
QUEUE_DROP_THRESHOLD = 30
READ_STALE_LAG_THRESHOLD = 2
REPLICA_CATCHUP_STEP = 2
LEADER_CHECK_INTERVAL = 1.0
LEADER_FAILOVER_DELAY = 2.0


class Message:
    def __init__(self, src, dst, payload, sent_at, retry=0):
        self.src = src
        self.dst = dst
        self.payload = payload
        self.sent_at = sent_at
        self.retry = retry
        self.delivered = False
        self.dropped = False


class Request:
    def __init__(
        self,
        attempt_id,
        root_id,
        src,
        dst,
        request_type,
        sent_at,
        timeout=DEFAULT_REQUEST_TIMEOUT,
        retry=0,
    ):
        self.id = attempt_id
        self.root_id = root_id
        self.src = src
        self.dst = dst
        self.request_type = request_type
        self.sent_at = sent_at
        self.timeout = timeout
        self.retry = retry
        self.delivered = False
        self.completed = False
        self.committed = False
        self.timed_out = False
        self.dropped = False
        self.stale_read = False
        self.commit_index = None
        self.response_at = None
        self.latency = None


class Node:
    def __init__(self, env, node_id, role, network):
        self.env = env
        self.id = node_id
        self.role = role
        self.network = network
        self.status = HEALTHY
        self.load = random.randint(10, 45)
        self.health = 100
        self.inbox = simpy.Store(env)
        self.request_queue = simpy.Store(env)
        self.log = []
        self.queue_length = 0
        self.last_commit_index = 0
        self.replica_lag = 0 if role in ("Primary", "Replica") else None
        self.last_heartbeat_at = env.now

        self.env.process(self._run())
        self.env.process(self._receive())
        self.env.process(self._serve_requests())

    def _is_shed(self):
        """Check if this node has been shed for load shedding."""
        return self.network.is_node_shed(self.id)

    def _run(self):
        while True:
            yield self.env.timeout(random.uniform(*HEARTBEAT_INTERVAL))
            if self.status == FAILED:
                continue

            
            if self._is_shed():
                continue

            jitter = random.randint(-4, 4)
            if self.queue_length > 0:
                jitter += min(6, self.queue_length)
            self.load = max(0, min(100, self.load + jitter))

            n_nodes = len(self.network.nodes)
            peers = [
                nid for nid in self.network.nodes
                if nid != self.id and not self.network.is_node_shed(nid)
            ]
            
            hb_chance = min(1.0, 5.0 / max(1, n_nodes))
            if peers and random.random() < hb_chance:
                target = random.choice(peers)
                self.send(target, f"heartbeat from {self.id} at t={self.env.now:.1f}")

            
            req_rate = min(0.45, 2.5 / max(1, n_nodes))
            if random.random() < req_rate and peers:
                self._generate_request()

    def _generate_request(self):
        
        healthy_nodes = [
            n for n in self.network.nodes.values()
            if n.status != FAILED and not self.network.is_node_shed(n.id)
        ]
        if not healthy_nodes:
            return

        
        n_nodes = len(self.network.nodes)
        write_pct = 0.30 if n_nodes <= 5 else (0.20 if n_nodes <= 10 else 0.12)
        req_type = "read" if random.random() > write_pct else "write"

        if req_type == "write":
            target = self.network._smart_write_target()
        else:
            consensus_nodes = [
                n.id for n in healthy_nodes if n.role in ("Primary", "Replica")
            ]
            if consensus_nodes:
                target = random.choice(consensus_nodes)
            else:
                target = random.choice([n.id for n in healthy_nodes])

        self.network.submit_request(self.id, target, req_type)

    def _receive(self):
        while True:
            msg = yield self.inbox.get()
            if self.status == FAILED:
                continue
            msg.delivered = True
            self.last_heartbeat_at = self.env.now
            self._log(f"  <- received '{msg.payload}' from {msg.src}")

    def _serve_requests(self):
        while True:
            req = yield self.request_queue.get()
            self.queue_length = max(0, self.queue_length - 1)

            if self.status == FAILED:
                req.dropped = True
                continue

            service_time = self.network.base_latency + 0.12 + (self.queue_length * 0.08)
            service_time *= 1.0 + (self.load / 180.0)
            if self.status == DEGRADED:
                service_time *= 1.15
            yield self.env.timeout(service_time)

            if self.network.is_root_finished(req.root_id):
                continue

            if req.request_type == "write":
                self.network._complete_write_request(self, req)
            else:
                self.network._complete_read_request(self, req)

    def send(self, dst_id, payload):
        msg = Message(self.id, dst_id, payload, self.env.now)
        self.env.process(self.network.transmit(msg))
        self._log(f"  -> sent '{payload}' to {dst_id}")
        return msg

    def enqueue_request(self, req):
        self.queue_length += 1
        self.request_queue.put(req)

    def _log(self, text):
        entry = f"[t={self.env.now:6.1f}] [{self.id}/{self.role}] {text}"
        self.log.append(entry)

    def get_state(self):
        return {
            "id": self.id,
            "role": self.role,
            "status": self.status,
            "load": self.load,
            "health": self.health,
            "queue_length": self.queue_length,
            "replica_lag": self.replica_lag,
            "last_commit_index": self.last_commit_index,
        }

    def __repr__(self):
        return f"Node({self.id}, {self.role}, {self.status}, load={self.load}%, q={self.queue_length})"


class Network:
    def __init__(self, env, base_latency=0.5, packet_loss_rate=BASE_PACKET_LOSS):
        self.env = env
        self.base_latency = base_latency
        self.packet_loss_rate = packet_loss_rate
        self.nodes = {}
        self.partitions = set()
        self.message_log = []
        self.request_log = []

        self.metrics = {
            "request_timeouts": 0,     
            "timeout_events": 0,        
            "request_retries": 0,
            "writes_attempted": 0,
            "writes_committed": 0,
            "writes_rejected_no_quorum": 0,
            "reads_completed": 0,
            "stale_reads": 0,
            "quorum_available_ticks": 0.0,
            "quorum_unavailable_ticks": 0.0,
            "leader_elections": 0,
            "split_brain_events": 0,
        }

        self.spike_active = False
        self.spike_multiplier = 1.0

        self.current_primary = None
        self.commit_index = 0

        self._root_seq = 0
        self._attempt_seq = 0
        self._last_primary_failure_at = None
        self.last_leader_heartbeat = env.now

       
        self.request_roots = {}

        
        self._shed_node_ids = set()

        env.process(self._consensus_loop())
        env.process(self._replica_catchup_loop())

    
    def is_node_shed(self, node_id):
        """Check if a node is currently shed. Used by Node._is_shed() and routing."""
        
        if node_id in self._shed_node_ids:
            return True
        try:
            from llm_agent import _shed_nodes
            return node_id in _shed_nodes
        except ImportError:
            return False

    def mark_node_shed(self, node_id):
        """Mark a node as shed (called by llm_agent._execute_shed)."""
        self._shed_node_ids.add(node_id)

    def mark_node_readmitted(self, node_id):
        """Mark a node as re-admitted (called by llm_agent._execute_readmit)."""
        self._shed_node_ids.discard(node_id)

   
    def add_node(self, node):
        self.nodes[node.id] = node
        if node.role == "Primary":
            if self.current_primary is None:
                self.current_primary = node.id
            else:
                
                node.role = "Replica"
                node.replica_lag = max(0, self.commit_index - node.last_commit_index)

    def count_primaries(self):
        return sum(1 for n in self.nodes.values() if n.role == "Primary")

    def _enforce_single_primary(self):
        primaries = [n for n in self.nodes.values() if n.role == "Primary"]
        if len(primaries) <= 1:
            return

        self.metrics["split_brain_events"] += 1

        keeper_id = self.current_primary
        if keeper_id is None or keeper_id not in self.nodes:
            keeper_id = sorted(primaries, key=lambda n: n.id)[0].id

        for node in primaries:
            if node.id != keeper_id:
                node.role = "Replica"
                if node.replica_lag is None:
                    node.replica_lag = max(1, self.commit_index - node.last_commit_index)
                else:
                    node.replica_lag = max(node.replica_lag, 1)

        self.current_primary = keeper_id

    def set_primary(self, new_primary_id, count_election=True):
        if new_primary_id not in self.nodes:
            return None

        for node in self.nodes.values():
            if node.id == new_primary_id:
                node.role = "Primary"
                node.replica_lag = 0
            else:
                if node.role == "Primary":
                    node.role = "Replica"
                if node.role == "Replica":
                    node.replica_lag = max(0, self.commit_index - node.last_commit_index)

        self.current_primary = new_primary_id
        self.last_leader_heartbeat = self.env.now
        if count_election:
            self.metrics["leader_elections"] += 1
        return new_primary_id

    def rejoin_node(self, node_id, preferred_role=None, degraded=True, health=40.0, load=20.0):
        node = self.nodes.get(node_id)
        if not node:
            return None

        
        for other_id in self.nodes:
            if other_id != node_id:
                self.heal_partition(node_id, other_id)

        
        self.mark_node_readmitted(node_id)

        node.status = DEGRADED if degraded else HEALTHY
        node.health = health
        node.load = load
        node.queue_length = 0

        
        if preferred_role is not None:
            node.role = preferred_role
        elif node.role == "Primary":
            if self.current_primary is None or self.current_primary == node_id:
                self.set_primary(node_id, count_election=False)
            else:
                node.role = "Replica"

        if node.role == "Replica":
            if node.replica_lag is None:
                node.replica_lag = max(1, self.commit_index - node.last_commit_index)
            node.last_commit_index = max(0, self.commit_index - 1)
            node.replica_lag = max(1, self.commit_index - node.last_commit_index)

        self._enforce_single_primary()
        return node

    def _eligible_replicas(self):
        return [
            n for n in self.nodes.values()
            if n.role == "Replica" and n.status != FAILED
        ]

    def elect_new_primary(self):
        replicas = sorted(
            self._eligible_replicas(),
            key=lambda n: ((n.replica_lag or 0), n.id),
        )
        if not replicas:
            self.current_primary = None
            return None

        new_primary = replicas[0]
        return self.set_primary(new_primary.id, count_election=True)

    def _has_quorum(self):
        voters = [n for n in self.nodes.values() if n.role in ("Primary", "Replica")]
        if not voters or self.current_primary is None:
            return False

        total_voters = len(voters)
        primary = self.nodes.get(self.current_primary)
        if primary is None or primary.status == FAILED:
            return False

        reachable = 1  
        for n in voters:
            if n.id == self.current_primary:
                continue
            if n.status == FAILED:
                continue
            if (self.current_primary, n.id) in self.partitions or (n.id, self.current_primary) in self.partitions:
                continue
            reachable += 1

        return reachable >= (total_voters // 2) + 1

    def _smart_write_target(self):
        """
        Consensus-aware write routing: when quorum is tight or primary is
        degraded, proactively elect a healthier primary or defer writes
        to avoid commit failures.
        """
        primary = self.nodes.get(self.current_primary) if self.current_primary else None

        
        if primary is None or primary.status == FAILED:
            elected = self.elect_new_primary()
            return elected if elected else (self.current_primary or list(self.nodes.keys())[0])

        
        if primary.status == DEGRADED or primary.health < 40 or primary.load > 85:
            candidates = [
                n for n in self.nodes.values()
                if n.role == "Replica"
                and n.status == HEALTHY
                and n.health > primary.health
                and not self.is_node_shed(n.id)
            ]
            if candidates:
                best = max(candidates, key=lambda n: n.health)
                
                if best.health > primary.health + 20:
                    self.set_primary(best.id, count_election=True)
                    return best.id

        
        voters = [n for n in self.nodes.values() if n.role in ("Primary", "Replica")]
        total_voters = len(voters)
        healthy_voters = sum(
            1 for n in voters
            if n.status != FAILED
            and (self.current_primary, n.id) not in self.partitions
            and (n.id, self.current_primary) not in self.partitions
        )
        quorum_needed = (total_voters // 2) + 1

        
        if healthy_voters < quorum_needed and random.random() < 0.15:
            return None  

        return self.current_primary

    
    def transmit(self, msg):
        latency = self.base_latency + random.uniform(0, 0.25)
        yield self.env.timeout(latency)

        if msg.retry == 0:
            self.message_log.append(msg)

        if (msg.src, msg.dst) in self.partitions or (msg.dst, msg.src) in self.partitions:
            msg.dropped = True
            return

        if random.random() < self.packet_loss_rate:
            if self.spike_active and msg.retry < MAX_RETRIES:
                retry_delay = RETRY_BASE_DELAY * (RETRY_BACKOFF_MULT ** msg.retry)
                retry_msg = Message(msg.src, msg.dst, msg.payload, msg.sent_at, retry=msg.retry + 1)
                self.env.process(self._retry_transmit(retry_msg, retry_delay))
                return
            msg.dropped = True
            return

        dst_node = self.nodes.get(msg.dst)
        if dst_node:
            msg.delivered = True
            dst_node.inbox.put(msg)

    def _retry_transmit(self, msg, delay):
        yield self.env.timeout(delay)

        if (msg.src, msg.dst) in self.partitions or (msg.dst, msg.src) in self.partitions:
            msg.dropped = True
            return

        if random.random() < self.packet_loss_rate:
            if msg.retry < MAX_RETRIES:
                retry_delay = RETRY_BASE_DELAY * (RETRY_BACKOFF_MULT ** msg.retry)
                next_retry = Message(msg.src, msg.dst, msg.payload, msg.sent_at, retry=msg.retry + 1)
                self.env.process(self._retry_transmit(next_retry, retry_delay))
            else:
                msg.dropped = True
            return

        dst_node = self.nodes.get(msg.dst)
        if dst_node:
            msg.delivered = True
            dst_node.inbox.put(msg)

    
    def submit_request(self, src, dst, request_type, timeout=DEFAULT_REQUEST_TIMEOUT):
        
        if dst is None or self.is_node_shed(src) or self.is_node_shed(dst):
            return None

        self._root_seq += 1
        root_id = f"req-{self._root_seq}"

        self.request_roots[root_id] = {
            "src": src,
            "initial_dst": dst,
            "request_type": request_type,
            "sent_at": self.env.now,
            "timeout": timeout,
            "completed": False,
            "timed_out": False,
        }

        if request_type == "write":
            self.metrics["writes_attempted"] += 1

        req = self._make_attempt(root_id, src, dst, request_type, self.env.now, timeout, retry=0)
        self.request_log.append(req)
        self.env.process(self._dispatch_request(req))
        self.env.process(self._watch_root_timeout(root_id))
        return req

    def _make_attempt(self, root_id, src, dst, request_type, sent_at, timeout, retry):
        self._attempt_seq += 1
        return Request(
            attempt_id=f"{root_id}-a{self._attempt_seq}",
            root_id=root_id,
            src=src,
            dst=dst,
            request_type=request_type,
            sent_at=sent_at,
            timeout=timeout,
            retry=retry,
        )

    def is_root_finished(self, root_id):
        root = self.request_roots.get(root_id)
        if not root:
            return True
        return root["completed"] or root["timed_out"]

    def _dispatch_request(self, req):
        if self.is_root_finished(req.root_id):
            return

        yield self.env.timeout(self.base_latency + random.uniform(0, 0.15))

        if self.is_root_finished(req.root_id):
            return

        if (req.src, req.dst) in self.partitions or (req.dst, req.src) in self.partitions:
            req.dropped = True
            self.metrics["timeout_events"] += 1
            self._retry_request(req, reason="partition")
            return

        if random.random() < self.packet_loss_rate:
            req.dropped = True
            self.metrics["timeout_events"] += 1
            self._retry_request(req, reason="loss")
            return

        dst_node = self.nodes.get(req.dst)
        if not dst_node or dst_node.status == FAILED:
            req.dropped = True
            self.metrics["timeout_events"] += 1
            self._retry_request(req, reason="target_unavailable")
            return

        if dst_node.queue_length >= QUEUE_DROP_THRESHOLD:
            req.dropped = True
            self.metrics["timeout_events"] += 1
            self._retry_request(req, reason="queue_pressure")
            return

        req.delivered = True
        dst_node.enqueue_request(req)

    def _retry_request(self, req, reason="retry"):
        if self.is_root_finished(req.root_id):
            return

        root = self.request_roots.get(req.root_id)
        if not root:
            return

        deadline = root["sent_at"] + root["timeout"]
        if self.env.now >= deadline:
            return

        if req.retry >= MAX_RETRIES:
            return

        self.metrics["request_retries"] += 1
        next_target = self._choose_retry_target(req)
        next_req = self._make_attempt(
            req.root_id,
            req.src,
            next_target,
            req.request_type,
            root["sent_at"],
            root["timeout"],
            retry=req.retry + 1,
        )
        self.request_log.append(next_req)
        retry_delay = RETRY_BASE_DELAY * (RETRY_BACKOFF_MULT ** req.retry)
        self.env.process(self._delayed_request_retry(next_req, retry_delay))

    def _delayed_request_retry(self, req, delay):
        yield self.env.timeout(delay)
        if not self.is_root_finished(req.root_id):
            self.env.process(self._dispatch_request(req))

    def _choose_retry_target(self, req):
        if req.request_type == "write":
            return self.current_primary or req.dst

        
        candidates = [
            n.id for n in self.nodes.values()
            if n.status != FAILED
            and n.role in ("Primary", "Replica")
            and not self.is_node_shed(n.id)
        ]
        return random.choice(candidates) if candidates else req.dst

    def _watch_root_timeout(self, root_id):
        root = self.request_roots.get(root_id)
        if not root:
            return

        yield self.env.timeout(root["timeout"])

        root = self.request_roots.get(root_id)
        if not root or root["completed"] or root["timed_out"]:
            return

        root["timed_out"] = True
        self.metrics["request_timeouts"] += 1

        for req in self.request_log:
            if req.root_id == root_id and not req.completed:
                req.timed_out = True

    def _mark_root_completed(self, root_id):
        root = self.request_roots.get(root_id)
        if root:
            root["completed"] = True
            root["timed_out"] = False

    def _complete_write_request(self, node, req):
        if self.is_root_finished(req.root_id):
            return

        req.completed = True
        req.response_at = self.env.now
        req.latency = self.env.now - req.sent_at

        self._enforce_single_primary()

        primary = self.nodes.get(self.current_primary) if self.current_primary else None
        if not primary or primary.status == FAILED:
            return

        if node.id != primary.id:
            return

        if self.count_primaries() != 1:
            return

        if not self._has_quorum():
            self.metrics["writes_rejected_no_quorum"] += 1
            return

        self.commit_index += 1
        node.last_commit_index = self.commit_index
        node.replica_lag = 0

        req.committed = True
        req.commit_index = self.commit_index
        self.metrics["writes_committed"] += 1
        self._mark_root_completed(req.root_id)

        for replica in self.nodes.values():
            if replica.id == node.id or replica.role != "Replica" or replica.status == FAILED:
                continue
            if (node.id, replica.id) in self.partitions or (replica.id, node.id) in self.partitions:
                replica.replica_lag = max(replica.replica_lag or 0, 2)
            else:
                lag_add = 1 if replica.status == HEALTHY else 2
                replica.replica_lag = max(replica.replica_lag or 0, lag_add)

    def _complete_read_request(self, node, req):
        if self.is_root_finished(req.root_id):
            return

        req.completed = True
        req.response_at = self.env.now
        req.latency = self.env.now - req.sent_at
        self.metrics["reads_completed"] += 1
        self._mark_root_completed(req.root_id)

        lag = node.replica_lag or 0
        if node.role == "Replica" and lag >= READ_STALE_LAG_THRESHOLD:
            req.stale_read = True
            self.metrics["stale_reads"] += 1

   
    def partition(self, node_a, node_b):
        self.partitions.add((node_a, node_b))

    def heal_partition(self, node_a, node_b):
        self.partitions.discard((node_a, node_b))
        self.partitions.discard((node_b, node_a))

    
    def _consensus_loop(self):
        while True:
            yield self.env.timeout(LEADER_CHECK_INTERVAL)

            self._enforce_single_primary()

            primary = self.nodes.get(self.current_primary) if self.current_primary else None
            if primary and primary.status != FAILED:
                self.last_leader_heartbeat = self.env.now
                self._last_primary_failure_at = None
            elif self._last_primary_failure_at is None:
                self._last_primary_failure_at = self.env.now

            if self._has_quorum():
                self.metrics["quorum_available_ticks"] += LEADER_CHECK_INTERVAL
            else:
                self.metrics["quorum_unavailable_ticks"] += LEADER_CHECK_INTERVAL

            primary = self.nodes.get(self.current_primary) if self.current_primary else None
            if primary is None or primary.status == FAILED:
                failure_start = self._last_primary_failure_at or self.env.now
                if self.env.now - failure_start >= LEADER_FAILOVER_DELAY:
                    self.elect_new_primary()
                    self._last_primary_failure_at = None

    def _replica_catchup_loop(self):
        while True:
            yield self.env.timeout(1.0)

            primary = self.nodes.get(self.current_primary) if self.current_primary else None
            if not primary or primary.status == FAILED:
                continue

            for node in self.nodes.values():
                if node.role != "Replica" or node.status == FAILED or node.replica_lag is None:
                    continue

                if (primary.id, node.id) in self.partitions or (node.id, primary.id) in self.partitions:
                    node.replica_lag = max(node.replica_lag, 1)
                    continue

                if node.last_commit_index < self.commit_index:
                    node.last_commit_index = min(
                        self.commit_index,
                        node.last_commit_index + REPLICA_CATCHUP_STEP,
                    )

                node.replica_lag = max(0, self.commit_index - node.last_commit_index)

    
    def consensus_snapshot(self):
        self._enforce_single_primary()
        return {
            "current_primary": self.current_primary,
            "has_quorum": self._has_quorum(),
            "commit_index": self.commit_index,
            "leader_elections": self.metrics["leader_elections"],
            "split_brain_events": self.metrics["split_brain_events"],
        }

    def get_stats(self):
        total = len(self.message_log)
        delivered = sum(1 for m in self.message_log if m.delivered)
        dropped = sum(1 for m in self.message_log if m.dropped)
        rate = f"{(delivered / total * 100):.1f}%" if total > 0 else "0%"

        req_total = len(self.request_roots)
        req_completed = sum(1 for r in self.request_roots.values() if r["completed"])
        req_timeouts = self.metrics["request_timeouts"]
        timeout_events = self.metrics["timeout_events"]
        committed = self.metrics["writes_committed"]
        writes_attempted = self.metrics["writes_attempted"]
        stale_reads = self.metrics["stale_reads"]
        reads_completed = max(1, self.metrics["reads_completed"])

        timeout_rate = f"{(req_timeouts / max(1, req_total) * 100):.1f}%"
        commit_rate = f"{(committed / max(1, writes_attempted) * 100):.1f}%"
        stale_rate = f"{(stale_reads / reads_completed * 100):.1f}%"

        return {
            "total_messages": total,
            "delivered": delivered,
            "dropped": dropped,
            "success_rate": rate,
            "requests_total": req_total,
            "requests_completed": req_completed,
            "request_timeouts": req_timeouts,
            "timeout_events": timeout_events,
            "timeout_rate": timeout_rate,
            "writes_attempted": writes_attempted,
            "writes_committed": committed,
            "commit_rate": commit_rate,
            "reads_completed": self.metrics["reads_completed"],
            "stale_reads": stale_reads,
            "stale_read_rate": stale_rate,
            "request_retries": self.metrics["request_retries"],
            "quorum_available_ticks": self.metrics["quorum_available_ticks"],
            "quorum_unavailable_ticks": self.metrics["quorum_unavailable_ticks"],
            "leader_elections": self.metrics["leader_elections"],
            "split_brain_events": self.metrics["split_brain_events"],
            "current_primary": self.current_primary,
            "commit_index": self.commit_index,
        }


def build_cluster(env, config):
    network = Network(env)
    for cfg in config:
        node = Node(env, cfg["id"], cfg["role"], network)
        network.add_node(node)

    # final safety pass
    network._enforce_single_primary()
    if network.current_primary is None:
        replicas = [n for n in network.nodes.values() if n.role == "Replica"]
        if replicas:
            network.set_primary(sorted(replicas, key=lambda n: n.id)[0].id, count_election=False)

    return network