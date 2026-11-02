# Autonomous Recovery Agent — Live Cluster Implementation Plan

**Thesis:** *Can a recovery policy developed in simulation make correct, measurable
decisions against a real containerized distributed system — when it must diagnose the
failure itself, with no ground-truth labels?*

The pipeline under test:

```
real telemetry ──► diagnosis ──► simulated policy ──► real async action ──► independent evaluation
```

Nothing in the existing repo root is modified. `rule_based.py`, `llm_agent.py`, `scorer.py`,
`predictor.py`, `decision_memory.py`, and `config.py` are imported as-is by the new agent.

| | |
|---|---|
| Origin | Plan-mode plan, 2026-08-04 |
| Rev 2 | 2026-09-09 — load generator, voter/non-voter split, leadership gate |
| Rev 3 | 2026-09-09 — diagnosis layer, evaluation-only ground truth, intent/result split |
| Rev 4 | 2026-09-10 — Gate 0.2 answered; native readonly nodes; telemetry from `getStatus()` |
| Rev 5 | 2026-09-10 — incident identity, target generation, executor durability, warm-up rules, evaluator join |
| Rev 6 | 2026-09-10 — `boot_id` identity, transition-log durability, optimal matching, warm-up barrier |
| Rev 7 | 2026-09-10 — dummy assignments, `target_generation` demoted to metadata, `resolved_at` vs `duration_hint` |
| Rev 8 | 2026-09-10 — action-aware validation (§6.2a); isolation/shed are not restart-durable |
| Built | **ALL PHASES 1-8 COMPLETE — every gate green, 2026-10-17.** G3 8/0 · G4 14/0 · G5 12/0 (4/4 recall, 0 FP) · G6 13/0 ★freeze · G7 14/0 · G8 18/0. Fixtures: 22 classifier + 15 action-aware. Phase 9 dashboard BUILT (localhost:9700). |
| **Phase 6** | **GREEN 13/13, 2026-10-10 — ★ FREEZE POINT REACHED.** Crash injected, node recovered unattended (`StartedAt` advanced), full auditable chain. No LLM. Phase 7 next. |

---

## 1. Architecture

```
                    ┌─────────────────────────────────────────┐
                    │   Docker network: live_cluster           │
   failure_         │  ┌────────┐ ┌────────┐ ┌────────┐       │
   injector ───────►│  │ node-1 │◄┤ node-2 │◄┤ node-3 │ VOTERS │◄─ real Raft
                    │  └───┬────┘ └───┬────┘ └───┬────┘       │   (pysyncobj)
   load_        ───►│  ┌────────┐ ┌────────┐  NON-VOTERS       │
   generator        │  │ node-4 │ │ node-5 │  (sheddable)      │
   (weighted)       │  └───┬────┘ └───┬────┘                   │
                    └──────┼──────────┼────────────────────────┘
                           │ health 1 Hz
                           ▼
        ┌──────────────────── Kafka (KRaft) ──────────────────────┐
        │  cluster.health   cluster.actions   cluster.scores       │
        │  cluster.action_results        cluster.failures ◄── GT   │
        └───┬──────────────────────────────────────────▲──────────┘
            ▼                                           │
  ┌─────────────────────────────────┐                   │
  │        RECOVERY AGENT            │                   │
  │  RealNetworkAdapter (cache)      │                   │
  │            ▼                      │                   │
  │  classifier.diagnose()            │  ◄── NO ground truth
  │     → FailureDiagnosis            │                   │
  │            ▼                      │                   │
  │  rule_based.assess_confidence()   │                   │
  │     ≥75 → rule_based.decide()     │  no LLM           │
  │     <75 → llm_agent.decide_only() │──► Groq           │
  │            ▼                      │                   │
  │  scorer · predictor · memory      │───────────────────┘
  └────────────┬────────────────────┘   intents
               ▼ cluster.actions
     ┌───────────────────┐
     │     EXECUTOR      │──► docker API / gRPC AdminAction / docker exec
     │  validate → apply │──► cluster.action_results
     └───────────────────┘

            cluster.failures ──► EVALUATION (offline) ◄── everything else
```

**Critical rule: the agent never consumes `cluster.failures`.** Ground truth exists solely
as an offline label source for measuring diagnosis accuracy, detection latency, false
positives, and false recovery attempts. Feeding it to the agent would reduce the project to
"given a labeled failure, pick an action" — which the simulator already answers.

Seven services: `kafka`, `node-1..N`, `load_generator`, `recovery_agent`, `executor`,
`failure_injector`, and an offline `evaluator`.

---

## 2. Phase 0 — Decisions and prerequisites

**No code until these are answered in writing.**

### 0.1 Environment

```bash
docker --version && docker compose version    # Compose v2
docker info | grep -i "total memory"           # ≥4 GB
python3 -c "import groq; print(groq.__version__)"
grep GROQ_API_KEY .env
```

### 0.2 GATE — Does pysyncobj expose leadership transfer? ✅ ANSWERED 2026-09-10

**No.** Verified against `pysyncobj==0.3.12`.

Full public API: `addNodeToCluster removeNodeFromCluster addOnTickCallback
removeOnTickCallback conf destroy doTick encryptor forceLogCompaction getCodeVersion
getStatus hasQuorum isNodeConnected isReady otherNodes printStatus raftCommitIndex
raftCurrentTerm raftLastApplied readonlyNodes selfNode setCodeVersion waitBinded waitReady`.

Leadership-related names are all private or internal: `_getLeader`, `_isLeader`,
`__onBecomeLeader`, `__onLeaderChanged`. No `transfer`, no `stepDown`. A source grep for
`transfer|stepDown|step_down` matches only a docstring about message transport.

**Consequence — `promote_replica` becomes forced step-down.** Mechanism:

> Briefly isolate the current leader with the **existing partition machinery**
> (`iptables -j DROP` against its peers) for ~2× `raftMaxTimeout`, then heal. The remaining
> voters time out and elect a new leader; the old leader rejoins as a follower.

Chosen over `removeNodeFromCluster(self)` or `destroy()` because it touches no pysyncobj
internals and reuses code Phase 4 already builds. **You do not choose the winner** — log
the intent, the pre-state, and the actual elected node separately, and score against what
actually happened.

### 0.2b Findings that change the design

The same inspection turned up three capabilities the plan was about to reimplement:

| Finding | Effect |
|---|---|
| `readonlyNodes` — native non-voting members that still receive log replication | Replaces `RAFT_ENABLED=false` in §0.3 |
| `isNodeConnected(node)` + `getStatus()['partner_node_status_server_<id>']` | Replaces the custom N×N probe loop — `probes.py` is **deleted** |
| `getStatus()` returns quorum, leader, commit/applied indices, term, uptime, per-follower match index | Most of §4.6's telemetry comes free |

### 0.3 GATE — Voter / non-voter split

`_rank_shed_candidates()` ([llm_agent.py:326](llm_agent.py#L326)) refuses to shed any
`Primary` or `Replica`. If every node is a Raft voter, **load shedding can never fire.**

**Decision: split the tiers.**

| Nodes | Role | Raft | Sheddable |
|---|---|---|---|
| `node-1..3` | `Primary` (leader) / `Replica` | **voting** — `SyncObj(self_addr, partners)` | no |
| `node-4..N` | `Worker` / `Gateway`, alternating per [cluster_config.py:16-19](cluster_config.py#L16-L19) | **readonly** — `SyncObj(None, voter_addrs)` | yes |

**Revised in Rev 4:** use pysyncobj's native readonly nodes rather than disabling Raft.
A node connecting with no self-address registers as readonly (`transport.py:352-365`:
`readonly = node is None`); the leader adds it to `__readonlyNodes` and includes it in log
broadcast, but it never votes.

Strictly better than the `RAFT_ENABLED=false` stub:

- readonly nodes **receive real log replication**, so they serve genuinely fresh reads with
  a real `replica_lag = leader_commit_idx − last_applied` — not a local-cache approximation
- excluded from quorum math, so `hasQuorum` is unaffected and they stay sheddable
- `stale_read` becomes a real, measurable event on the serving tier

`generate_compose.py` always renders exactly 3 voters and `N−3` readonly nodes. Minimum N = 3.
Confirm the exact constructor form in the Phase 1 gate — a one-line check.

### 0.4 Pin versions — `live_cluster/requirements.txt`

```
pysyncobj==0.3.12
grpcio==1.68.1
grpcio-tools==1.68.1
confluent-kafka==2.6.1
docker==7.1.0
psutil==6.1.0
groq
python-dotenv

# evaluator only — offline, not shipped in any node image
scipy==1.14.1        # linear_sum_assignment for §8.1a optimal matching
```

Kafka `apache/kafka:3.9.0` (KRaft, no ZooKeeper). Base `python:3.11-slim`.

---

## 3. Directory layout

```
live_cluster/
  requirements.txt
  .env.example
  docker-compose.template.yml
  generate_compose.py
  proto/
    node.proto
    loadgen.proto
  common/
    kafka_io.py            # producer/consumer helpers, topic names
    schemas.py             # message dataclasses + validation
  node/
    Dockerfile             # + iproute2, iptables; NET_ADMIN
    node_service.py        # Raft + gRPC + health publisher
    health.py              # health score
    raft_status.py         # getStatus() → telemetry fields (§4.6)
    behaviors.py           # leak / cpu-burn / shed toggles
  load_generator/
    gen.py                 # weighted traffic + SetWeights RPC
  recovery_agent/
    agent_service.py       # consume health → diagnose → decide → emit intent
    network_adapter.py     # RealNetworkAdapter (NetworkLike shim)
    classifier.py          # FailureDiagnosis from telemetry only
    incidents.py           # open-incident suppression
    events.py              # LiveFailureEvent duck-type
  executor/
    executor_service.py    # validate → apply → publish results
  failure_injector/
    inject.py              # CLI chaos + ground-truth publisher
  evaluator/
    evaluate.py            # offline join of all five topics → metrics
  tests/
    fixtures/*.jsonl       # recorded cluster.health streams
    test_classifier.py
    test_adapter.py
    test_idempotency.py
    test_split_brain.py
    test_warmup.py
    test_evaluator.py
    test_transition_log.py
    test_action_aware.py
  scripts/
    smoke_test.sh          # calls wait_for_warmup.sh before ANY injection
    tail_topic.sh
    wait_for_warmup.sh     # blocks until every node clears §5.5 warm-up
```

---

## 4. Contracts

### 4.1 `cluster.health` — the only agent input

```jsonc
{
  "node_id": "node-2", "role": "Replica", "status": "degraded",
  "health": 62.0, "load": 41.0, "queue_length": 3,
  "raft_leader": false, "raft_term": 17, "commit_index": 1042,
  "ts": 1757400000.123,

  // process identity — see §4.1a
  "boot_id": "3f9a1c04-...",      // uuid4, generated once at process start
  "started_at": 1757399400.02,    // absolute epoch of process start
  "uptime": 600.1,                // duration; freshness only, never identity

  "apply_lag": 1,        // leader_commit_idx − last_applied — READ FRESHNESS (self-observed)
  "replica_lag": 1,      // alias of apply_lag; the name the existing decision code reads
  "match_lag": null,     // followers: null. Leader publishes the map below instead.

  "rss_mb": 340.2, "rss_baseline_mb": 180.0,   // memory_leak
  "cpu_pct": 41.0,                              // cpu_overload
  "req_p50_ms": 8.2, "req_p99_ms": 240.0,       // latency_spike
  "req_ok": 18, "req_fail": 1, "req_timeout": 0, // windowed, last 1s
  "peers_reachable": {"node-1": true, "node-3": false}  // partition
}
```

**Leader-only fields** (present when `raft_leader == true`):

```jsonc
{
  "match_lag": {"node-2": 0, "node-3": 4, "node-4": 1},  // leader_commit_idx − match_idx
  "leader_commit_idx": 1042
}
```

### 4.1a Process identity — `uptime` is a duration, not an identity

`uptime` cannot answer "is this the same process I decided against." It is a duration; it
tells you *how long*, never *which*. Every restart assertion and staleness check needs an
absolute anchor instead:

| Field | Type | Use |
|---|---|---|
| `boot_id` | uuid4, generated once at process start | **identity** — exact equality test. `boot_id != expected` ⇒ the process restarted. |
| `started_at` | absolute epoch of process start | **ordering** — `started_at > expected` ⇒ restarted *later*. Survives id comparison being unorderable. |
| `uptime` | seconds since start (pysyncobj `getStatus()['uptime']`) | **freshness only** — warm-up windows, logging. Never a restart proof. |

Both `boot_id` and `started_at` are generated by the node process itself, not read from
pysyncobj. Carry both: `boot_id` gives an exact test, `started_at` gives ordering when you
need to know *which* restart came first.

This replaces the `uptime < expected_target_uptime` heuristic from Rev 5 everywhere it
appeared (§5.5 re-warm, §6.2 staleness, §6.4 assertions).

### 4.1b Two lags, two questions

Do not collapse these — they answer different things and come from different observers:

| | `apply_lag` | `match_lag` |
|---|---|---|
| Formula | `leader_commit_idx − last_applied` | `leader_commit_idx − match_idx[node]` |
| Answers | **read freshness** — will this node serve a stale read? | **replication progress** — how far behind is this node's log? |
| Observed by | the node itself | the **leader** only |
| Published in | every node's own health message | the leader's health message, as a map |

A node can have `match_lag = 0` (has every entry) but `apply_lag > 0` (hasn't applied them
yet) — it is fully replicated and still serving stale reads. The reverse also occurs after
a rejoin, when a node applies quickly but is still catching up its log tail.

`replica_lag` is kept as an **alias of `apply_lag`** because the existing code's dominant
use is the stale-read check (`lag ≥ READ_STALE_LAG_THRESHOLD`,
[network.py:707-709](network.py#L707-L709)). `match_lag` is new telemetry for diagnosis and
evaluation only; nothing in the reused decision code reads it.

All counters are **windowed over 1 s**, never cumulative — the simulator's cumulative
`success_rate` is precisely why its `delivery_score` is dead (§8.3).

Fields arrive in stages: base fields Phase 1–2, `req_*` Phase 3, `rss_*` and
`peers_reachable` Phase 4.

**`peers_reachable` semantics:** asserted only by nodes alive enough to publish. A missing
row is **unknown**, never "reports everyone unreachable" — otherwise a crash looks like a
total partition.

### 4.2 Topics

| Topic | Producer | Consumer | Meaning |
|---|---|---|---|
| `cluster.health` | nodes, 1 Hz | agent, evaluator | observation |
| `cluster.actions` | agent | executor, evaluator | **decision emitted — never "completed"** |
| `cluster.action_results` | executor | evaluator | `accepted \| rejected \| failed \| completed` |
| `cluster.scores` | agent | evaluator | scorer record + `decision_id` |
| `cluster.failures` | injector | **evaluator only** | ground-truth label |

`cluster.actions` payload:

```jsonc
{
  "decision_id": "sha1(incident_id|attempt_no)[:16]",
  "incident_id": "node-2:crash:1757400000123",   // target:type:opened_at_ms
  "attempt_no": 1,                                // 1 = initial, 2+ = follow-up
  "action": "restart_node", "target": "node-2",
  "source": "rule", "confidence": 82,
  "reason": "...", "decided_at": 1757400001.5,

  // target-specific staleness contract (§6.2)
  "target_generation": 12,
  "expected_target_status": "failed",
  "expected_boot_id": "3f9a1c04-...",     // identity, not duration (§4.1a)
  "expected_started_at": 1757399400.02,

  "diagnosis": {"failure_type": "crash", "confidence": 0.91,
                "evidence": ["..."], "ambiguous": false}
}
```

`cluster.action_results` payload:

```jsonc
{"decision_id": "...", "incident_id": "...", "state": "completed",
 "detail": "container restarted, up 0.4s",
 "applied_at": 1757400002.1, "rejected_reason": null}
```

`state` ∈ `accepted | applying | completed | failed | rejected | reconciled` (§6.4).

`cluster.failures` payload — **two records per incident**, injection and resolution:

```jsonc
{"event": "injected", "gt_id": "gt-17",
 "failure_type": "network_partition", "target": "node-1<->node-2",
 "injected_at": 1757400000.0,
 "duration_hint": 5.0}          // what the injector INTENDED; descriptive only

{"event": "resolved", "gt_id": "gt-17",
 "resolved_at": 1757400005.2,
 "resolved_by": "injector"}      // injector | recovery | unresolved
```

**`duration_hint` is never a time boundary.** It is what the injector planned, not what
happened, and for three of the five failure types it is not even knowable at injection
time — a `crash`, `leak`, or `cpu` fault is undone by *the recovery system under test*,
not by the injector. Only `partition` and `latency` are self-healing on a timer.

So `resolved_at` has two provenances, and the evaluator must keep them distinct:

| `resolved_by` | Source | Applies to |
|---|---|---|
| `injector` | published by the injector when it removes the fault (heal partition, drop the `tc` qdisc) — **authoritative** | `partition`, `latency` |
| `recovery` | **derived by the evaluator** from `cluster.health` (node healthy again, or `boot_id` changed) — mark it derived in the report | `crash`, `leak`, `cpu` |
| `unresolved` | the run ended with the fault still active | any |

Never substitute `duration_hint` for `resolved_at`. A crash whose hint says 5 s but whose
recovery took 14 s would silently truncate the matching window and manufacture a "missed
incident."

### 4.3 `proto/node.proto`

```proto
syntax = "proto3";
package livecluster;

service NodeService {
  rpc GetStatus   (StatusRequest) returns (StatusReply);
  rpc SubmitWrite (WriteRequest)  returns (WriteReply);
  rpc SubmitRead  (ReadRequest)   returns (ReadReply);
  rpc AdminAction (AdminRequest)  returns (AdminReply);
}

message WriteRequest { string key = 1; string value = 2; }
message WriteReply   { bool committed = 1; int64 commit_index = 2; string error = 3; }
message ReadRequest  { string key = 1; }
message ReadReply    { string value = 1; bool stale = 2; int32 lag = 3; }

enum AdminVerb {
  ISOLATE = 0; REJOIN = 1;
  START_LEAK = 2; STOP_LEAK = 3;
  START_CPU_BURN = 4; STOP_CPU_BURN = 5;
  ENTER_SHED = 6; EXIT_SHED = 7;
  // 8 = TRANSFER_LEADERSHIP — RESERVED, NOT IMPLEMENTED.
  //     Gate 0.2 confirmed pysyncobj exposes no leadership transfer.
  //     promote_replica uses forced step-down instead (§0.2, Phase 8).
  ENSURE_RUNNING = 9;        // assertion form, see §6.4
}
message AdminRequest { AdminVerb verb = 1; string arg = 2; }
message AdminReply   { bool ok = 1; string detail = 2; }
```

### 4.4 `proto/loadgen.proto`

```proto
service LoadGenService {
  rpc SetWeights (WeightTable) returns (WeightReply);
  rpc GetWeights (Empty)       returns (WeightTable);
  rpc SetRate    (RateRequest) returns (WeightReply);
}
message WeightTable { map<string, double> weights = 1; }
```

### 4.5 Health score — `node/health.py`

```
health = 100
       − min(40, replica_lag * 8)
       − min(30, windowed_failure_rate * 30)
       − min(30, max(0, (rss_mb − rss_baseline_mb) / 10))
clamp [0, 100]

status = failed    if health < 20
         degraded  if health < 70
         healthy   otherwise
```

The **agent** additionally marks a node `failed` when its last health message is older
than 5 s — that is how `docker kill` is detected, since a dead node publishes nothing.

### 4.6 Telemetry from `SyncObj.getStatus()` — `node/raft_status.py`

Most consensus telemetry is already computed by pysyncobj; do not reimplement it.

| `getStatus()` key | Maps to |
|---|---|
| `has_quorum` | `consensus_snapshot()['has_quorum']` |
| `leader` | `current_primary`; `raft_leader = (leader == self)` |
| `commit_idx` | `commit_index` |
| `leader_commit_idx − last_applied` | **`apply_lag`** (= `replica_lag` alias) — self-observed, voters and readonly alike |
| `leader_commit_idx − match_idx_server_<id>` | **`match_lag`** — leader-observed only, published as a map (§4.1b) |
| `raft_term` | increments on every election → derive `leader_elections` from term changes rather than counting manually |
| `partner_node_status_server_<addr>` (2 = connected, 0 = not) | **`peers_reachable`** — **address-keyed**, each node reports its own view. Voters report the other voters; readonly nodes report all three voters. |
| `readonly_node_status_server_<n>` | **unusable for attribution — see §4.6a.** `<n>` is an opaque counter, not a node identity. |
| `match_idx_server_<addr>` | **`match_lag`, voters only** — leader-observed. Readonly entries appear here too but counter-keyed and anonymous, so they are filtered out. |
| `uptime` | **freshness only** — warm-up windows. Restart proofs use `boot_id` (§4.1a), which the node generates itself. |
| `state` | raft state, for logging |

### 4.6a The serving tier self-reports — verified 2026-09-10

**Probe finding.** A readonly node has no self-address, so pysyncobj identifies
it by an auto-incrementing counter (`transport.py:362`,
`nodeId = str(self._readonlyNodesCounter)`). The leader therefore emits
`readonly_node_status_server_0` and `match_idx_server_0` — it genuinely cannot
say *which* readonly node those describe, and the counter is not stable across
reconnects.

So the leader's view of the serving tier is discarded. **Readonly nodes report
their own state instead**, and a live 3-voter + 1-readonly probe confirms they
have everything needed:

| Readonly node's own `getStatus()` | Value |
|---|---|
| `leader_commit_idx`, `last_applied` | present → **`apply_lag` is self-observable** |
| `partner_node_status_server_<addr>` | present for **all three voters**, address-keyed |
| `has_quorum`, `leader`, `raft_term` | present |
| `self` | `None` — this is what marks it readonly |

This does not weaken §4.1b, it sharpens it: `apply_lag` was always defined as
self-observed, and `match_lag` as leader-observed. The correction is only that
**`match_lag`'s domain is voters, never the serving tier.** Parsers must filter
`match_idx_server_*` and `partner_node_status_server_*` to address-shaped keys
(containing `:`) and drop counter-shaped ones.

The probe also confirmed the §0.3 bet: the readonly node received replicated
values (`k1='v1'`), and `has_quorum` stayed `True` with 3 voters — so the
serving tier really does serve fresh reads without touching quorum math.

### 4.7 Split-brain — define it on `(node_id, raft_term, leader)`

**Correction to Rev 4's first draft.** "Structurally 0" was too strong. Raft guarantees *at
most one leader per term*. It does **not** guarantee that at most one node *believes* it is
leader at a given wall-clock moment: during a partition, an old leader in term T can retain
a locally valid leadership view while the majority elects a new leader in term T+1. Two
nodes then publish `raft_leader: true` simultaneously. Safety still holds — the stale
leader has no quorum and commits nothing — but the observation is real.

So record every `(node_id, raft_term, raft_leader)` tuple and derive **two** metrics:

| Metric | Definition | Expected |
|---|---|---|
| `split_brain_violations` | two **distinct nodes** claiming leadership **in the same term** | **0.** Nonzero means a bug in our observation or in pysyncobj — investigate, never report as normal. |
| `stale_leader_window_ms` | duration of simultaneous leadership claims **in different terms** | **Nonzero during partitions.** Expected and interesting. |

The second is a genuine measurement the simulator cannot produce: how long a partitioned
old leader keeps a stale self-view before stepping down. The simulator's
`split_brain_events` ([network.py:288-307](network.py#L288-L307)) has no term concept at
all — it counts `role == "Primary"` collisions and repairs them — so it is closer to the
second metric than the first, and the two are not comparable.

Report as: *"zero same-term violations; stale-leader windows of X ms observed during
partitions, resolved without any committed divergence."*

---

## 5. The diagnosis layer

### 5.1 `FailureDiagnosis`

```python
@dataclass
class FailureDiagnosis:
    target: str
    failure_type: str          # crash | network_partition | latency_spike
                               # | memory_leak | cpu_overload | ambiguous
    confidence: float          # 0.0-1.0
    evidence: list[str]
    ambiguous: bool
    observed_at: float
    target_generation: int     # metadata only — see §6.2
```

Forcing a precise type on insufficient evidence would make the evaluation misleading, so
`ambiguous` is a first-class outcome, not a failure of the classifier.

### 5.1a MEASURED — serving-tier crashes have no corroboration (2026-09-26)

The Phase 5 gate **missed a real crash** of node-4, and §4.6a is why: readonly nodes are
anonymous to Raft peers (`readonly_node_status_server_0`), so their reachability rows are
filtered out and **no voter can name them**. Rule 1 requires *all peers agree it is
unreachable*; with zero peers able to express an opinion, `known == 0` and it can never
fire for the serving tier.

Rule 1b closes the gap: a node whose own process publishes its heartbeat and has gone
silent is almost certainly dead, so call it a crash — at **0.70, not 0.90**, with evidence
that states the corroboration is structurally impossible rather than merely absent.

**The branch is gated on role, deliberately.** For a *voter*, peers normally can and do
name it, so `known == 0` means their silence is itself unexplained — inferring a crash
there would be exactly the "missing row means unreachable" mistake §4.1 warns against. The
fixture suite asserts both halves: serving-tier silence → crash@0.70, voter silence with
no peer opinion → nothing.

### 5.2 Rules, evaluated in order over a ~15 s rolling window

**Peer status requires hysteresis.** `isNodeConnected()` is a point-in-time check; a single
failed probe is not a partition. Every rule reading `peers_reachable` requires
**K = 3 consecutive samples** (≈3 s at 1 Hz) agreeing before it fires.

Hysteresis is deliberately **asymmetric**: 3 samples to *declare* a partition, 1 sample to
*clear* it. That biases the classifier toward inaction, which is the correct bias when the
downstream action can cost availability.

| # | Condition | Emits | Conf |
|---|---|---|---|
| 1 | node stale >5 s **and** every live peer reports it unreachable for ≥3 consecutive samples | `crash` | 0.90 |
| 1b | **serving-tier** node stale >5 s **and no peer can name it** (§5.1a) | `crash` | 0.70 |
| 2 | node **still publishing** **and** its `raft_term` exceeds the cluster median by ≥3 **and** the leader's `match_lag[node]` is growing monotonically over ≥3 samples | `network_partition` | 0.85 |
| 3 | `rss_mb − rss_baseline_mb` rising monotonically over ≥5 samples, slope above threshold | `memory_leak` | scale by slope × r² |
| 4 | `cpu_pct` > 85 sustained ≥3 samples, rss flat | `cpu_overload` | 0.80 |
| 5 | `req_p99_ms` > 3× that node's own **same-leadership-regime** baseline **and** > 50 ms absolute, cpu and rss flat (§5.2a) | `latency_spike` | 0.75 |
| 6 | node stale >5 s **but** peers can still reach it | `ambiguous` | 0.40 — publisher/telemetry fault, not node failure |
| 7 | health declining, no rule dominates | `ambiguous` | < 0.50, evidence lists what was seen |

### 5.2a MEASURED — leadership changes the latency regime (2026-09-26)

§5.2 already baselined latency per-node, because the leader's p99 sits ~1000×
a follower's just by serving writes. Phase 5 gate run 2 showed that is only
half the guard.

Latency was injected on the leader (node-3). The delay cost node-3 its
leadership; **node-2 was promoted**, began serving writes, and its p99 went
0.03 ms → 198 ms — the quorum round-trip. Measured against node-2's own
*follower* history, that read as a **6585× spike**: a false positive, on a node
with no fault, caused entirely by a regime change. The gate scored it
`wrong_target 1`, `latency_spike 0/1`.

Two corrections, both now fixture-locked:

1. **Baseline samples must come from the same leadership regime.** Filter the
   baseline window on `raft_leader` matching the current value. A just-promoted
   node therefore has no same-regime history and **abstains** until it builds
   one. Same class of rule as the `boot_id` re-warm (§5.5): a regime change
   invalidates prior history, and the honest response is to abstain rather than
   to compare across it.
2. **An absolute floor of 50 ms, not a ratio alone.** A ratio against a
   near-zero baseline explodes meaninglessly — 0.03 ms → 0.09 ms is a "3×
   spike" no client could perceive. The `6585×` figure was the tell.

### 5.2b MEASURED — `req_p99_ms` cannot detect injected latency (2026-10-10)

**Two hypotheses, both wrong, both corrected by measurement.** Recorded in full because
the wrong turns are the finding.

The §5.2a fix worked — `wrong_target` and `false_positive` both went to 0 — but the gate
then **missed the real latency injection** (`latency_spike 0/1`) at +400 ms, and again at
+150 ms.

**First hypothesis (wrong):** latency is undetectable because +400 ms deposes the leader
and invalidates the baseline. Half right — a leadership change does invalidate the
baseline — but it is not the binding constraint.

**Second hypothesis (wrong):** there is a usable band `50 ms < delay < raftMinTimeout`,
so inject 150 ms. Measured result:

| | leader p99 | ratio | leadership |
|---|---|---|---|
| baseline | 154.07 ms | — | held |
| +150 ms injected | 349.30 ms | **2.27x** | **held** |

So 150 ms holds leadership (hypothesis 2's good half) but reaches only 2.27x against a
3.0x threshold.

**The actual constraint.** Write p99 grows at only **~1.3x the injected delay** (+195 ms
observed for +150 ms applied), because the injected delay is one leg of a round trip
already dominated by a ~154 ms quorum baseline. Reaching 3x therefore needs ~237 ms —
and 400 ms is measured to destabilise leadership. The band is not narrow, it is
**effectively empty**: every delay large enough to clear the ratio is close enough to
Raft's own failure detector to be unsafe, and the 3x threshold cannot be lowered without
giving back the false positives §5.2a just eliminated.

**Conclusion: `req_p99_ms` is the wrong instrument for this fault.** It measures
end-to-end write latency, in which an injected path delay is diluted by the quorum
baseline and entangled with leadership. The honest fix is the **peer-RTT probe** flagged
in Phase 3 and deferred: each node timing its own round trip to each peer. There a
150 ms delay appears as ~150 ms on a ~1 ms baseline — a 150x signal instead of 2.27x —
and it is independent of who happens to be serving writes.

**Until that probe exists, `latency_spike` is not detectable and the gate should say so**
rather than being tuned until it passes. Reported recall is 3/4 types, with the missing
one explained by instrumentation rather than by classifier error.

### 5.2c MEASURED — the peer-RTT probe works, and what it costs (2026-10-10)

Replacing rule 5's instrument with a direct peer-RTT probe produced the first
**complete 4/4 recall** the project has had: `latency_spike 1/1`, where every
write-path attempt had scored 0/1. The signal is ~150x (151 ms on a ~1 ms
baseline) instead of 2.27x.

It also introduced **three false positives**, both mechanisms worth recording
because neither is obvious from the design.

**1. In-process probing cannot separate "slow path" from "too busy to measure".**
The probe thread lives in the node's own process, so a CPU burn starves it and
inflates its own outbound RTT to *every* peer. Measured: node-5 reported
`rtt to 4/4 peers elevated` **three seconds before** `cpu_overload` confirmed —
the latency rule won the race against the rule that actually explained the
fault. Fix: a saturated node defers to `cpu_overload`. (The old write-path rule
had this guard; it was dropped in the rewrite.)

**2. "Most peers" was implemented as "at least half".** `len(elevated) * 2 <
measured` lets 2-of-4 and 2-of-3 through, and both leaked. A node seeing one or
two slow peers is observing *their* problem; only a strict majority implicates
its own path. Fix: `<=`.

**Standing limitation to state in any write-up.** The probe measures the path
*through this node's event loop*, so it conflates network delay with local
saturation by construction. The CPU guard resolves the overlap in favour of the
more specific explanation, but a node that is both saturated and genuinely
behind a slow link will report only the saturation. An out-of-process prober
would not have this problem, and would also not have the attribution matrix
that makes rule 5 work — a real trade, not an oversight.

### 5.3 `ambiguous` routes correctly with zero new code

`assess_confidence` does `base_confidence.get(failure_type, 50)`
([rule_based.py:63-72](rule_based.py#L63-L72)). An unrecognized type returns 50 ≥
`TIER2_THRESHOLD` (45) → **tier 2, LLM decides**; penalties may push it to tier 3, also an
LLM path. `RULE_DEFAULTS.get(type, "monitor_only")` degrades sensibly, so
`diverges_from_rule` stays meaningful.

Uncertain diagnosis therefore escalates to the reasoning path automatically. Put
`evidence[]` in the prompt.

### 5.4 Incident suppression — `incidents.py`

A memory leak persists for many windows; re-emitting a diagnosis every second would flood
the policy. Keep one **open incident** per `(node, failure_type)`, opened on first
diagnosis and closed when the **diagnosis has been absent for ≥5 consecutive cycles**.
Re-diagnosis of an open incident does not emit a new event. Mirrors
`predictor._should_emit_alert` ([predictor.py:69](predictor.py#L69)).

**MEASURED CORRECTION (2026-09-26).** This section originally closed an incident when the
node returned to `healthy` for ≥3 samples. That is the wrong signal: `status` is derived
from the health score, and **a latency spike never moves it**. So every brief dip below the
3× threshold closed the incident and the next sample opened a fresh one — one injected
latency spike produced **three incidents**, two of which the evaluator then counted as
false positives (Phase 5 gate run 1: 3 TP, 3 FP from 4 injections).

Absence of the diagnosis is the only thing that actually means "over", and a window of 5
rides out the flicker any threshold comparison has near its boundary. Node status stays
available to the tracker and is deliberately unused.

Each incident carries `incident_id = f"{target}:{failure_type}:{opened_at_ms}"` and a
monotonic `attempt_no`. These are the identity anchor for every downstream decision (§6.1)
and the join key the evaluator groups on (§8.1a). The incident — not the health sample and
not the state version — is the unit of work.

### 5.5 Warm-up and minimum-sample rules

Without these, container startup manufactures diagnoses.

| Rule | Requirement |
|---|---|
| **Global warm-up** | Emit nothing for a node's first **60 s** after its first health message. Suppresses boot transients. |
| **RSS baseline** | `rss_baseline_mb` = median RSS over samples in the 30–60 s window after start — never the first reading, which is pre-cache-fill. |
| **Leak rule (3)** | ≥8 samples past baseline **and** r² ≥ 0.9 on the linear fit, in addition to the slope threshold. |
| **Latency rule (5)** | ≥20 requests in the window, or `req_p99_ms` is undefined and the rule **abstains** (contributing to `ambiguous`, never to a spurious `latency_spike`). Rolling baseline needs ≥60 s of history. |
| **Post-restart re-warm** | When a node's `boot_id` **changes**, it re-enters warm-up. (Keyed on identity, not on `uptime` decreasing — a node that restarts twice between samples still shows exactly one identity change.) |

The last is not optional. A freshly restarted container's RSS climbs steeply as caches
refill; without re-warm it satisfies the leak rule within seconds of *every* recovery,
producing a restart → leak-diagnosis → restart oscillation.

### 5.5a MEASURED — `peers_reachable` cannot detect a partition (2026-09-14)

Rev 4 built rule 2 on `peers_reachable`. **Phase 4 measurement shows that does
not work**, for two independent reasons. Both were found by injecting real
partitions, not by reasoning.

**1. `isNodeConnected()` is socket-level, so it is unreliable — but not inert.**
`syncobj.py:671` is literally `node in self.__connectedNodes`, a set driven by
TCP connect/disconnect callbacks. A DROP rule discards packets without a RST,
so an **already-ESTABLISHED** connection gets no disconnect callback; the
kernel just retransmits with exponential backoff (~15 min at default
`tcp_retries2`).

Two measurements, and they disagree — which is the point:

| Scenario | `peers_reachable` | Why |
|---|---|---|
| single-link cut, node stays in a working cluster | **stayed `True` for 45 s** | the established connection is never torn down, so nothing re-connects and nothing fails |
| majority cut, node campaigning | **flipped `False` within 14 s** | the isolated node churns connections while campaigning; a *fresh* connect to a DROPped IP fails fast, unlike an established one |

So the flag is not dead — it is **conditional on connection churn**, which makes
it exactly the wrong thing to build a rule on: it reports correctly in the case
that is already obvious and stays silent in the subtle one. Rule 2 keys on term
divergence and `match_lag` growth because those are semantic, fast, and do not
depend on whether TCP happened to notice.

**2. A single-link cut in a 3-voter cluster leaves no lasting trace at all**,
because Raft correctly routes around it. Cutting node-1↔node-2 (the leader)
moved leadership to node-3 — the node that could still see both — and the
cluster then returned to `term` stable, `apply_lag` 0, `match_lag` all 0.

> **This is the system working, not the classifier failing.** A diagnosis layer
> that reports *no incident* for an absorbed partition is **correct**. Injecting
> single-link partitions and expecting a diagnosis would manufacture "missed
> incident" counts that penalise the classifier for being right (§8.1a).

**What a partition that actually matters looks like.** Cutting node-1 from
*both* peers (a majority cut) produced three strong, sustained signals:

| Signal | Isolated node | Majority |
|---|---|---|
| `raft_term` | 14 → 28 → **51**, climbing — campaigning forever, never winning | stays 14 |
| leader's `match_lag[node-1]` | 0 → 53 → **129**, monotonic | others 0 |
| `has_quorum` | false | true |

So rule 2 keys on **term divergence + monotonic `match_lag` growth** — semantic
Raft symptoms that move within seconds — not on a socket flag that lies for
minutes. `peers_reachable` stays in the telemetry (it is free, and it *is*
meaningful when a process actually dies) but it is no longer load-bearing for
partition detection.

**One more measured consequence:** on heal, the isolated node returns with term
66 and *deposes the sitting leader*, forcing a fresh election. Isolation is not
free even after it ends — worth remembering for Phase 8's forced step-down,
which uses exactly this mechanism.

**Injector implication:** `inject.py partition --a X --b Y` cuts one link.
For a *detectable* partition the gate must isolate a node from every peer.

### 5.6 Known fidelity limit — partition detection is easier here than in reality

`iptables DROP` targets peer IPs; Kafka is a different container with a different IP, so
**a partitioned node keeps publishing telemetry throughout.** Rule 2 exploits exactly that.

This is an artifact of partitioning the data plane while leaving the observability plane
intact. Real partitions frequently take telemetry with them, collapsing rules 1 and 2 into
silence-vs-silence. So the honest claim is *asymmetric-reachability detection*, not
partition detection in general.

Optional hard case: `inject.py partition --blackout` also DROPs the Kafka subnet. Report as
a separate evaluation row; **do not let it gate any phase.**

---

## 6. Execution model

**`cluster.actions` is an intent. `cluster.action_results` is the outcome.** Nothing else
may be read as "the action happened."

### 6.1 Identity — anchor on the incident, not the state version

`sha1(target | failure_type | observed_state_version)` was wrong: the same node hitting the
same failure type at a repeating version collides, and it cannot distinguish an initial
decision from a follow-up within one incident.

**Anchor on incident identity instead.** `incidents.py` (§5.4) already maintains one open
incident per `(node, failure_type)`; give it an explicit id at open time:

```
incident_id  = f"{target}:{failure_type}:{opened_at_ms}"
attempt_no   = monotonic counter within the incident (1 = initial, 2+ = feedback follow-up)
decision_id  = sha1(f"{incident_id}|{attempt_no}")[:16]
```

Carry `incident_id` and `attempt_no` as explicit fields as well, so the evaluator never has
to reverse a hash to group decisions by incident. Kafka redelivery collides naturally;
genuinely distinct decisions never do.

### 6.2 Target-specific staleness — validated by fresh read

A global `observed_state_version` cannot answer "did *this* target change." The intent
therefore carries a target-scoped expectation:

| Field | Meaning | Executor validates? |
|---|---|---|
| `expected_target_status` | the status the decision was made against | **yes** |
| `expected_boot_id` | the target's process identity at decision time (§4.1a) | **yes** |
| `expected_started_at` | absolute start epoch, for ordering | **yes** |
| `target_generation` | agent-maintained counter, incremented only on a relevant change to that node | **no — metadata only** |

**`target_generation` is agent/evaluator metadata, not an executor check.** The executor
does not consume `cluster.health` and holds no generation history, so it has nothing to
compare a generation against; validating on it would be theatre. It stays in the payload
because the *agent* uses it for its own supersede logic and the *evaluator* uses it to
reconstruct which state version a decision was made against — but the executor must ignore
it. **The fresh `GetStatus()` checks below are the actual safety boundary.**

`target_generation` increments on: `status` transition, `role` change, **`boot_id` change**
(a restart happened), or shed-state change. Explicitly **not** health drift 62→61, load
jitter, or `commit_index` advancing — those are continuous and would churn it every tick.

**The executor validates against a fresh read, not a cache.** It is about to touch the node
anyway, so one `GetStatus(target)` at apply time is nearly free and is stronger than any
cached view:

1. `decision_id` in the durable store → **rejected** (`duplicate`)
2. **Action-aware state validation** — see §6.2a. A `boot_id` change or a status mismatch
   means different things for different actions, and for some actions the node's state is
   not the relevant state at all.
3. A decision for the same `target` with a **later `decided_at`** already applied →
   **rejected** (`superseded`). Ordering comes from `decided_at`, which is in the payload —
   not from `target_generation`, which the executor cannot interpret.
4. Otherwise → **accepted**, then §6.3

### 6.2a Validation is action-aware

**`already_satisfied` may only be returned when the observed change *is* the action's
effect.** A `boot_id` change proves a restart happened; it proves nothing about whether
traffic was rerouted or a node was shed. Applied unconditionally it would silently drop
every unrelated intent that happened to straddle a restart.

Worse, some actions don't act on the node at all, so gating them on node reachability is a
category error. Classify by **what the action acts on** and **what would prove it done**:

| Action | Acts on | `boot_id` changed | Target failed / unreachable |
|---|---|---|---|
| `restart_node`, `ENSURE_RUNNING` | container, via Docker API | **`already_satisfied`** — this *is* the effect | **precondition — proceed** |
| `promote_replica` (forced step-down) | leader's peers, via iptables | **`already_satisfied`** — a leader restart forces the election the action wanted | **`already_satisfied`** — a crashed leader already triggered it |
| `isolate_node` | node, via gRPC + iptables | effect **undone** (see below) — revalidate; never satisfied | **`already_satisfied`** — a failed node is already out of the cluster |
| `load_shed` / `readmit` | node (gRPC) **and** weight table | node-side shed flag **cleared** — revalidate; never satisfied | `already_satisfied` if `failed`; else **`unreachable`** |
| `reroute_traffic`, `rebalance_load` | **load generator only** | orthogonal — **skip the node gate entirely** | **not a blocker** — routing traffic away from a dead node is exactly correct |
| `monitor_only` | nothing | n/a | n/a — never rejected |

For every row where the change is not the action's effect, a changed premise is **`stale`**,
not `already_satisfied`. The two rejections mean opposite things downstream: `already_satisfied`
says the goal is met, `stale` says re-decide.

For the weight-table actions the executor validates against the **load generator** instead —
is the target still present in the weight table? — because that is the state those actions
actually mutate.

**Isolation and shed state do not survive a restart.** A container restart recreates the
network namespace, so `iptables` DROP rules vanish, and the node process's in-memory shed
flag is gone with it. That is why those rows say *undone* rather than *satisfied*, and it
has a consequence beyond validation: an isolated node that later restarts silently rejoins.
The agent re-observes and re-decides on the next cycle; nothing durable is lost, but the
effect is not sticky and should not be assumed to be.

The `boot_id` check is still what makes `restart_node` safe to redeliver without converting
it to an assertion — the restart is self-evidencing, and unlike the `uptime` comparison it
replaced, it is an identity test rather than a duration heuristic. It just does not
generalize past the actions whose effect it actually witnesses.

### 6.3 Durability — `accepted` is not completion

If the executor applies a restart and dies before publishing `completed`, redelivery would
see `duplicate` and the audit trail would be permanently ambiguous about whether the action
ran. Fix with a write-ahead record:

**Append-only means append a transition record — never rewrite a line.** Each state change
is its own line; the in-memory index folds the log into latest-state per `decision_id`:

```jsonl
{"decision_id":"a1b2","state":"accepted", "action":"restart_node","target":"node-2","ts":1757400002.0}
{"decision_id":"a1b2","state":"applying", "ts":1757400002.1}
{"decision_id":"a1b2","state":"completed","detail":"boot_id 3f9a→7c21","ts":1757400004.8}
```

```
before applying   → append "accepted", then "applying"; flush + fsync; publish "accepted"
after applying    → append "completed" or "failed"; publish the same
on startup        → fold the log; any decision_id whose latest state is "applying" is
                    orphaned → determine actual state via GetStatus / docker inspect,
                    append and publish "reconciled" with the finding
```

The **fsync before the side effect** is what makes it write-ahead at all — a buffered write
lost in the crash would void the entire guarantee.

Rewriting lines in place would defeat both the append-only property and the audit trail:
the transition log also gives you per-stage timing (queue delay, apply duration) for free.

`reconciled` is a distinct state precisely so the ambiguity is **visible in the audit
trail** rather than papered over. `seen_decision_ids` is derived from the folded index, so
persistence stops being optional.

Redelivery of a decision whose latest state is `applying` must **not** re-apply; it
reconciles instead.

### 6.4 Prefer assertions over imperatives

`restart_node` is not effect-idempotent — applied twice, it restarts twice. Where cheap,
express commands as desired state:

| Imperative | Assertion form |
|---|---|
| `restart_node` | `ENSURE_RUNNING(node, not_boot_id = X)` — "running, and not the boot I saw" |
| `load_shed` | `node.shedding = true` |
| `isolate_node` | `node.isolated = true` |
| `reroute_traffic` | stays a delta — not convertible |

Note that the two middle assertions are **desired state that a restart silently clears**
(§6.2a): the network namespace and the in-process shed flag both die with the container.
The assertion framing is still correct — it just means the desired state is not sticky, and
re-asserting it after a restart is a legitimate new decision rather than a duplicate.

---

## 7. Phases

Each phase has a gate. Do not proceed until it passes.

> Numbering shifted from Rev 2: the diagnosis layer is now its own Phase 5, so the
> **principal demo checkpoint is Phase 6** (previously "Phase 5 minus LLM").

### Phase 1 — Skeleton: Kafka + N nodes + heartbeat

Build `generate_compose.py` (renders Kafka KRaft + `node-1..N`, static hostnames, ports
`8000+i`, `NODE_PEERS` = the 3 voter addresses, `NODE_ROLE`, `RAFT_MODE=voter|readonly`
(§0.3), label `live_cluster.role=node`, `cap_add: [NET_ADMIN]`, per-node `mem_limit`), a
gRPC `GetStatus` stub, a 1 Hz dummy health publisher, and `common/`.

**Gate**
```bash
python generate_compose.py --nodes 3 && docker compose up -d
./scripts/tail_topic.sh cluster.health          # 3 node_ids, ~3 msg/s
docker compose down && python generate_compose.py --nodes 5 && docker compose up -d
./scripts/tail_topic.sh cluster.health          # 5 nodes; 1-3 voters, 4-5 Worker/Gateway
```

### Phase 2 — Real Raft

`pysyncobj.SyncObj` replicated dict on the 3 voters; `node-4..N` join as **readonly**
(§0.3). `SubmitWrite` → Raft `set()`, returns `committed` only after commit. `SubmitRead`
local, `stale=true` when `replica_lag ≥ 2`. Build `raft_status.py` and project the §4.6
fields into the health payload — including `peers_reachable`, which arrives free here
rather than in Phase 4.

**Gate**
```bash
# 1. readonly tier registered, not voting
./gate02/bin/python -c "..."               # confirm SyncObj(None, partners) constructor form
docker compose exec node-1 python -c "print(sync.getStatus())" \
  | grep -E "readonly_nodes_count|partner_nodes_count"
# expect partner_nodes_count=2, readonly_nodes_count=N-3

# 2. real election — ALL FOUR must hold
docker kill $(docker ps -qf name=node-1)   # kill the leader
./scripts/tail_topic.sh cluster.health
```

| # | Assertion | Why |
|---|---|---|
| 1 | a different node reports `raft_leader == true` | leadership actually moved |
| 2 | its `raft_term` > the pre-kill term, **and then stabilizes** | a real election, not flapping |
| 3 | `has_quorum == true` on the new leader | the majority side is intact |
| 4 | `commit_index` **advances after** the new leader is established | the true liveness proof — it can commit |
| 5 | readonly nodes keep replicating (`apply_lag` bounded) | the serving tier survived the failover |

Term increment alone is **not** sufficient: Raft bumps the term on *candidacy*, not on
victory, so a node that repeatedly stands and loses increments terms without any leader
ever being established. Assertion 2's stabilization clause and assertion 4 together are
what separate a successful election from election flapping.

### Phase 3 — Load generator (+ `req_*` telemetry)

N worker threads issuing reads/writes (default 20 rps, 20% writes), targets sampled from a
weight table, uniform at start. Writes always to the current leader; reads by weight.
`SetWeights` mutates live. Nodes begin reporting `req_ok/fail/timeout` and `req_p50/p99_ms`.

**Gate**
```bash
grpcurl -d '{"weights":{"node-4":0.0,"node-5":0.6}}' loadgen:9000 SetWeights
./scripts/tail_topic.sh cluster.health     # node-4 load → idle, node-5 rises, within ~10s
```
If load does not visibly move, every load-based action downstream is fiction. Fix it here.

### Phase 4 — Failure injector (+ `rss_*`, `peers_reachable`)

`inject.py`, discovering containers by Docker label:

| Command | Mechanism |
|---|---|
| `crash --target node-2` | `docker kill` |
| `partition --a node-1 --b node-2 [--blackout]` | `iptables -j DROP` both directions via `docker exec` |
| `latency --target node-2 --ms 300 --duration 5` | `tc qdisc add dev eth0 root netem delay` |
| `leak --target node-4 --rate-mb 5` | `AdminAction(START_LEAK)` — real allocation |
| `cpu --target node-5` | `AdminAction(START_CPU_BURN)` — real burn |

Every command publishes an `injected` ground-truth record to `cluster.failures` **before**
acting, carrying a fresh `gt_id`. For `partition` and `latency` the injector also publishes
the matching `resolved` record when it removes the fault (§4.2); for `crash`, `leak`, and
`cpu` it cannot — those are undone by the recovery system, so the evaluator derives
resolution from `cluster.health`. Nodes add
`rss_mb` and `rss_baseline_mb`. `peers_reachable` needs **no probe loop** — it is projected
straight out of `getStatus()`'s `partner_node_status_server_*` / `readonly_node_status_server_*`
keys (§4.6).

Note: real latency injection is **per-node**, while the simulator's is global
(`target="network"`). Live is strictly more precise, and `_normalize_target`'s `"network"`
branch ([llm_agent.py:202-208](llm_agent.py#L202-L208)) becomes dead code in live mode.

**Gate**
```bash
python inject.py latency --target node-2 --ms 300 --duration 10
./scripts/tail_topic.sh cluster.health     # node-2 p99 spikes, health degrades
./scripts/tail_topic.sh cluster.failures   # exactly one ground-truth event
python inject.py partition --a node-1 --b node-2
# node-1.peers_reachable["node-2"] == false while node-2 KEEPS publishing
```

### Phase 5 — Diagnosis layer (no actions taken)

Build `network_adapter.py`, `classifier.py`, `incidents.py`, `events.py`, and
`evaluator/evaluate.py`. The agent runs read-only: consumes `cluster.health`, emits
diagnoses to the log, **takes no action and reads no ground truth**.

`RealNetworkAdapter` duck-types what the decision code reads:

| Existing code expects | Adapter provides |
|---|---|
| `.nodes: dict[str, NodeLike]` | rolling `cluster.health` cache |
| `.id/.role/.status/.health/.load/.queue_length/.replica_lag` | from the health message |
| `.get_stats()` | **percent-formatted strings** (`"92.3%"`) — [scorer.py:102-105](scorer.py#L102-L105) parses with `.replace("%","")` |
| `.consensus_snapshot()` | primary, quorum, commit_index, elections, split-brain |
| `.is_node_shed(id)` | local shed set |
| mutators | publish intents (Phase 6) |

`LiveFailureEvent` matches [failures.py:19](failures.py#L19): `.failure_type .target .time
.resolved .resolved_at .resolve()`.

**Gate — this produces the project's first real numbers**
```bash
./scripts/smoke_test.sh --diagnose-only     # inject all 5 types, 3 runs each
python evaluator/evaluate.py --report diagnosis
```
Report per §8.1a's matching rules: per-type accuracy, detection latency over true positives,
steady-state false-positive rate, wrong-target rate, and `ambiguous` rate split by target
correctness. Record the baseline here — every later phase is measured against it.

Run a **no-injection control** first: 10 minutes of steady traffic with zero faults. The
false-positive count must be 0. If warm-up rules (§5.5) are wrong, this is where it shows,
and it costs ten minutes instead of a phase.

### Phase 6 — Policy (rules only) + executor ★ PRINCIPAL DEMO CHECKPOINT ✅ 13/13

**Measured 2026-10-10.** `docker kill node-4` → diagnosis `crash` @0.70 via rule 1b
(serving tier, no corroboration available) → `rule_based` tier 1 @82% → intent
`restart_node` with `expected_boot_id` → executor validated against a fresh `GetStatus`,
applied, published `accepted`→`applying`→`completed` → node-4 returned with a new
`StartedAt`. Three lines in the write-ahead transition log.

**Two bugs this gate caught, both since fixed:**

1. **Topics were never pre-created.** §4.2 specified it; the code relied on
   auto-creation. The executor subscribed to a non-existent `cluster.actions`, the topic
   was created when the agent produced the intent, and the executor's partition
   assignment resolved `auto.offset.reset=latest` to offset 1 — *past* the only intent,
   at offset 0. The consumer-group query was what proved it (`CURRENT-OFFSET=-`,
   `LOG-END-OFFSET=1`); the logs showed nothing at all. Fix: `ensure_topics()` at
   startup **and** the executor consuming from `earliest`, which is safe by construction
   because deterministic `decision_id`s (§6.1) plus the transition log (§6.3) exist
   precisely so at-least-once redelivery is harmless. A dropped intent is far worse than
   one offered twice.
2. **`applying` was logged but never published.** The gate's own check was *named* for
   three states and asserted only two, so it passed on `['accepted','completed']` while
   `applying` never reached the topic. The evaluator reads per-stage timing off the
   topic, not the executor's local file. Both the publish and the assertion are fixed.

Wire diagnosis → the exact routing from [main.py:633-678](main.py#L633-L678):

```python
confidence, tier, reason = rules.assess_confidence(adapter, event)
if tier == 1:
    summary, action = rules.decide(adapter, event, now)   # no LLM
else:
    pass                                                   # Phase 7
```

Time is **seconds**, not ticks. Executor implements validation (§6.2) and three actions:
`restart_node` (Docker API), `isolate_node` (`AdminAction(ISOLATE)` + iptables),
`monitor_only`.

**Gate**
```bash
./scripts/wait_for_warmup.sh          # BLOCKS until every node clears §5.5 warm-up
python inject.py crash --target node-2
# unattended, in order:
#   cluster.health         node-2 goes stale
#   agent log              diagnosis: crash, conf 0.9x, evidence listed — no ground truth read
#   cluster.actions        {restart_node, node-2, source:"rule", decision_id,
#                           incident_id, expected_boot_id, expected_target_status:"failed"}
#   cluster.action_results {accepted} → {completed}
#   docker ps              node-2 uptime resets
#   cluster.health         node-2 returns, commit_index catches the leader
```

**The warm-up barrier is mandatory, not decorative.** §5.5 suppresses all diagnoses for a
node's first 60 s, so a script that injects immediately after `docker compose up` gets
silence — and the gate looks like it hung or like the classifier missed the incident, when
in fact the system behaved correctly. Every gate that injects must block on
`wait_for_warmup.sh` first. That script polls the agent's per-node warm-up state and exits
0 only when all nodes have cleared, printing the wait so the delay is visibly intentional:

```
waiting for warm-up: node-1 ✓  node-2 ✓  node-3 (18s remaining) ...
all nodes warm after 61s — injecting
```

`WARMUP_SECONDS` is configurable for demo runs, but the Phase 5 control run and the
recorded baseline **must** use the production default. Lowering it reintroduces exactly the
startup false positives the rule exists to prevent, so any lowered-warm-up run is a demo
artifact, never a reported result.

**Freeze and tag this.** It demonstrates the entire claim with no LLM in it: real telemetry
interpreted, decision made without labels, action executed asynchronously, outcome
independently evaluated. Everything after is an upgrade to the policy layer.

### Phase 7 — LLM tier 2/3, scorer, predictor, memory

Tier 2/3 calls `llm_agent.decide_only(adapter, event, now)` unmodified — few-shot,
`decision_memory` injection, and multi-turn self-critique come along free. Buffer events in
a 500 ms window, then `decide_batch()`.

**Per-action feedback windows.** `feedback.py` hardcodes 5 ([feedback.py:51](feedback.py#L51))
and counts `restart_node` successful only at `health ≥ 70` ([feedback.py:92-95](feedback.py#L92-L95)).
A real container at t+5 s is often still booting and catching up its Raft log, so it reads
as failed → LLM follow-up → another restart → oscillation. Wrap with a per-action table:

| Action | Window |
|---|---|
| `reroute_traffic`, `rebalance_load` | 3 s |
| `isolate_node` | 5 s |
| `load_shed` / `readmit` | 8 s |
| `restart_node` | 15 s (boot + Raft catch-up) |

Scoring: keep the 4-component score computed **internally** — it is not a valid sim-vs-real
comparison metric, but `decision_memory.get_relevant_context()` sorts by it
([decision_memory.py:85](decision_memory.py#L85)) to pick the top-5 outcomes injected into
future prompts. Drop it and cross-run learning silently stops working.

Bind-mount the repo root so `decision_history.json` persists and is shared with the simulator.

**MEASURED, 2026-10-11 — two contract defects the Phase 7 gate exposed.**

**1. The repo mount must be `:rw`.** Mounting `..:/simulator:ro` makes
`memory.save()` hit a read-only filesystem, and `decision_memory.py:42`
swallows the `IOError` with a warning — so cross-run learning silently writes
nothing while appearing to work.

A nested single-file `:rw` bind over the `:ro` parent was tried first, on
least-privilege grounds, and **does not work on Docker Desktop for macOS**:
`docker inspect` reports `rw=true` on the inner mount, yet the write still
fails `EROFS`, because the parent read-only bind makes the underlying
filesystem read-only at the VM layer. Verified by inspection, not assumed. The
repo therefore mounts `:rw`.

**The check that was supposed to catch this passed instead.** It counted lines
with `wc -l`, and a file seeded with `[]` is one line — so it reported
"1 lines" and went green while the agent logged a read-only warning on every
cycle. It now parses the JSON and asserts a nonzero ENTRY count, plus a second
check that no `could not save decision history` warning appears at all. A line
count was never evidence that anything had been learned.

**2. An isolated node must report `status="isolated"`.**
`feedback.evaluate_action_result` judges `isolate_node` by
`status in ["failed", "isolated"]` ([feedback.py:97-100](feedback.py#L97-L100)).
The live node reported `status="healthy"` with a separate `isolated=True`
field, so **every** isolation was judged a failed action and triggered a
pointless follow-up intent. Observed in the gate as a second intent reading
`follow-up: Node not isolated — status=healthy`.

This is the "unmodified decision brain" assumption biting exactly where it was
predicted to: the reused code's success criteria encode the simulator's status
model, and the live telemetry has to speak that vocabulary rather than its own.
The feedback loop was working correctly — it was the contract underneath that
disagreed.

**Constraint:** `llm_agent` keeps cooldown/cache/shed state in module globals
([llm_agent.py:18-40](llm_agent.py#L18-L40)). Run exactly one agent process. Do not replicate.

**MEASURED, 2026-10-11 — `decide_only()` skips the action-sanity validator.**

`validate_action` ([llm_agent.py:211](llm_agent.py#L211)) is called by
`decide_and_recover()`, not by `decide_only()`. The live policy uses
`decide_only()` deliberately — `decide_and_recover()` executes against the
network it is handed, and the adapter's mutators raise by design — so the guard
rail was being skipped entirely.

Observed consequence: the model answered `monitor_only` for a `memory_leak` at
~35% health, which `validate_action` rejects outright
([llm_agent.py:232-233](llm_agent.py#L232-L233): *"Cannot monitor_only a memory
leak with health below 60%"*). The live system accepted a decision the
simulator would have blocked — so the two were not running the same policy, and
any comparison between them was not like-for-like.

The live policy now calls `validate_action` explicitly after `decide_only`.

**A defect in the reused code, reproduced rather than fixed.** The simulator's
response to a rejected action is to set it to `monitor_only`
([llm_agent.py:874](llm_agent.py#L874)) — which is incoherent when
`monitor_only` is itself what was rejected: the fallback is the action the guard
just refused. The live system reproduces this exactly, because silently
improving it would mean no longer running the policy under test. Flagged here as
a finding about the simulator, not a bug in the harness.

**Also fixed: the intent recorded the ROUTER's reason, not the model's.**
`assess_confidence` returns a confidence rationale ("memory leak needs
judgement, not pure rules"), and that string was being written into the intent's
`reason` field for LLM decisions — attributing the router's words to the model's
choice in every audit trail. The model's own reason is now carried, with the
router's preserved separately as `router_reason`.

**Gate**
```bash
python inject.py leak --target node-4 --rate-mb 5
# diagnosis: memory_leak (or ambiguous early, sharpening as slope establishes)
# cluster.actions with source:"llm" and a reason string
# cluster.scores ~5s later; decision_history.json grows
```

### Phase 8 — Full action set

1. `reroute_traffic` — executor calls `SetWeights`, halving the target's share. Real now.
2. `rebalance_load` — flatten weights across healthy, non-shed nodes.
3. `load_shed` / `readmit` — `AdminAction(ENTER_SHED)`: return gRPC `UNAVAILABLE` for new
   requests, drain in-flight, **keep Raft participation**; executor also zeroes the weight.
4. `promote_replica` — **forced step-down** per 0.2: isolate the leader for ~2×
   `raftMaxTimeout` via the partition machinery, heal, then record which node the cluster
   actually elected. Score against the observed outcome, never the intended target.

   **Stricter precondition than diagnosis.** This action deliberately creates an
   unavailability window, so it requires **K = 5** consecutive confirming samples, not the
   classifier's 3, plus `hasQuorum == true` among the remaining voters at decision time.
   A destructive action's precondition should always be more conservative than the
   diagnosis that suggests it.
5. `restart_node` on leak/CPU — verify the restart genuinely clears leaked RSS. It will;
   this is strictly more honest than the simulator's `health = 90`.

**Remove `reassign_role` from the live prompt.** Under real Raft there is no assignable
role. Override the action list in the agent's prompt, not in `llm_agent.py` — the simulator
still needs it.

**Gate:** `scripts/smoke_test.sh` — all 5 failure types, asserting the full chain
(ground truth → degradation → diagnosis → intent → result → recovery → score).

### Phase 9 — Dashboard ✅ BUILT 2026-10-17

`dashboard/` on **http://localhost:9700**. Stdlib only — no Flask, so no image
gains a dependency. Live node grid (health bars, leadership, quorum, shed and
isolated state), a decisions panel surfacing rule-vs-model divergence, and the
**causal timeline** as the centrepiece:

```
truth      node-4  INJECTED crash          ground truth — the agent never sees this
diagnosis  node-4  DIAGNOSED crash  70%    silent 7.4s; no peer can attest...
decision   node-4  DECIDED restart_node
action     node-4  EXECUTED completed      boot_id ? -> d539cc4c
score      node-4  SCORED 84               speed 70 · stability 95 · cascade 100 · delivery 70
```

Ground truth is displayed **and labelled as such**. The dashboard is the
observer, not the agent; §1 forbids labels on the agent's INPUT path, and
showing the answer key beside the agent's answer is the point of the exercise.

**New topic `cluster.diagnoses`.** Diagnoses were only ever logged, so the
chain could not be reconstructed from the wire — and the evaluator had to
scrape `docker compose logs`, which breaks on any format change. Both now read
a topic.

**What the dashboard found in its first five minutes — `speed_score` was always 0.**

`scorer.py:78-80` zeroes speed outright when `resolved` is false, and nothing
in the live system ever called `LiveFailureEvent.resolve()` — a method written
in Phase 5 and never wired. Speed is **35% of the total**, so every score the
live system ever produced was understated by up to 35 points. Worse,
`decision_memory.get_relevant_context()` sorts by score to choose which past
outcomes get injected into future prompts, so the distortion fed straight back
into the policy.

Measured before and after on identical crash-and-recover:

| | speed | stability | cascade | delivery | total |
|---|---|---|---|---|---|
| before | **0** | 95 | 100 | 70 | **59** |
| after | **70** | 95 | 100 | 70 | **84** |

A completed executor action now resolves the event — the recovery ran. Whether
it WORKED stays a separate question, answered by the feedback check.

**No gate caught this**, because every gate asserted a score *exists* and none
asserted it was *sane* — §8.4's pattern, seventh instance. It argues for the
dashboard beyond demo value: one view holding the whole chain makes an
implausible number obvious in a way per-stage assertions do not.

---

## 8. Evaluation

### 8.1 Primary metrics

1. **Diagnosis accuracy** per failure type, and **detection latency** (ground-truth ts → diagnosis ts).
   Report hysteresis separately: the 3-sample requirement (§5.2) adds a floor of ≈3 s to
   partition and crash detection **by design**. Subtract it when comparing against the
   simulator, or the tradeoff reads as classifier sluggishness rather than a precision choice.
2. **Action agreement** — same state, does the policy choose what the simulator chose?
3. **MTTR ranking** across failure types (ordering, not absolute values)
4. **Recovery success rate**
5. **Behavior under uncertainty** — what happens on `ambiguous`, and does escalation help?

Absolute scores are omitted from the comparison (§8.3).

### 8.1a Joining unlabeled diagnoses to ground truth

The evaluator sees two independent streams and must match them without the agent ever
having seen a label.

**Globally optimal one-to-one, not greedy.** Greedy nearest-first is order-dependent as
soon as incidents overlap or cascade — a diagnosis claimed early by a near-in-time but
wrong-target incident can starve the correct match, and the reported accuracy then depends
on arrival order rather than on classifier quality. Solve it as a min-cost assignment
instead (`scipy.optimize.linear_sum_assignment`; the matrix is a handful of rows per run,
so cost is irrelevant):

```
cost(D, G) = |D.observed_at − G.injected_at|
           + TARGET_PENALTY  if D.target != G.target
           + TYPE_PENALTY    if D.failure_type != G.failure_type
           = BIG             if D is outside G's window
```

`D` is feasible for `G` when `G.injected_at ≤ D.observed_at ≤ min(G.injected_at + W_max,
G.resolved_at)`, using the **`resolved_at`** from §4.2 — never `duration_hint`.
`W_max` = 30 s. Ties broken deterministically on `(G.injected_at, D.observed_at, D.target)`.

**The matrix must carry dummy assignments, or nothing can go unmatched.**
`linear_sum_assignment` produces a *maximum* matching: on a bare n_D × n_G matrix every
diagnosis is forced onto some incident, which would fabricate wrong-type/wrong-target
matches and drive false positives and missed incidents to zero by construction. Pad it to
square:

```
                   n_G real          n_D dummies
              ┌──────────────────┬──────────────────┐
   n_D real   │  cost(D, G)      │  diag UNMATCHED_D│   "this diagnosis is a false positive"
              │                  │  off-diag  BIG   │
              ├──────────────────┼──────────────────┤
 n_G dummies  │ diag UNMATCHED_G │        0         │   "this incident was missed"
              │ off-diag  BIG    │                  │
              └──────────────────┴──────────────────┘
```

Choose `BIG > UNMATCHED_D + UNMATCHED_G` so the solver always prefers a dummy over an
infeasible pairing, and use a large finite sentinel rather than `inf` — scipy raises on
matrices that are infeasible-infinite.

**All four constants are evaluation hyperparameters and must be reported alongside every
result:** `TARGET_PENALTY`, `TYPE_PENALTY`, `UNMATCHED_D`, `UNMATCHED_G`. The first two
decide how readily a cascade neighbour is attributed to an incident; the last two set the
price at which the solver prefers declaring a false positive or a miss over accepting a
poor match. Together they move every column of the results table. An unreported constant
makes the accuracy numbers unreproducible.

| Outcome | Condition |
|---|---|
| **true positive** | matched, `D.target == G.target`, `D.failure_type == G.failure_type` |
| **wrong type** | matched on target + window, type differs |
| **wrong target** | `D` inside `G`'s window but `D.target != G.target` — cascade or misattribution; count separately, never as a plain false positive |
| **ambiguous** | `D.ambiguous == true` — its own row. Split into *correct target* (partial credit — escalation was warranted) and *wrong target*. Never scored as "wrong type." |
| **false positive** | `D` with no `G` in window — the steady-state noise metric |
| **missed** | `G` with no `D` in window |

Detection latency = `D.observed_at − G.injected_at`, computed over **true positives only**;
including wrong-type matches would conflate speed with correctness.

Cascades need care: one injected failure can legitimately degrade neighbours, so a `D` on a
neighbour is *wrong target* rather than a false positive. Report the two separately — the
distinction is the difference between "hallucinated a failure" and "correctly saw the blast
radius but misattributed the cause."

### 8.2 Report the 2×2, not just end-to-end

|  | recovery succeeded | recovery failed |
|---|---|---|
| **diagnosis correct** | policy works | **isolates policy error** |
| **diagnosis wrong** | **action robust to misdiagnosis** | compounding failure |

The bottom-left cell will be well populated, because `restart_node` happens to fix crash,
memory leak, *and* CPU overload. Diagnosis accuracy and recovery success are therefore only
loosely coupled, and end-to-end numbers alone would flatter the system. The cell is also a
genuine finding in its own right: which actions are robust under diagnostic uncertainty.

Caveat to state explicitly: classifier and policy consume the same telemetry, so their
errors **correlate**. A miscomputed health score degrades both at once. The failure modes
are not independent.

### 8.3 Why absolute scores are not comparable

- Sim runs in **ticks**, live in **seconds**.
- `speed_score = 120 − mttr·10` ([scorer.py:78](scorer.py#L78)) is calibrated to tick-scale
  MTTR; a 12-second real restart scores 0 on a formula expecting sub-12 *ticks*.
- The sim's `success_rate` is **cumulative** over the whole run
  ([network.py:781-784](network.py#L781-L784)), so a 5-tick delta barely moves it and
  `delivery_score` converges to its neutral 70 regardless of action. (`pre_total_msgs` is
  captured at [scorer.py:46](scorer.py#L46) and never used — the windowed version was
  evidently intended.) Live telemetry is windowed, so that 15% of the score behaves
  differently by construction.

Compare **derived** metrics, never shared raw timing constants.

---

## 8.4 GATE AUDIT — assertions that tested a proxy (2026-10-11)

Six defects in this build were found sitting inside a **passing** check. That is
not bad luck, it is one systematic weakness: *an assertion that tests a proxy
will eventually certify the opposite of what it claims.* A pass over all seven
gates, asking of each check "does this test the property or a stand-in for it?",
found the following.

| Defect | The proxy that hid it |
|---|---|
| `health` hardcoded to `100.0` for eight phases | every gate asserted `rss_mb` / `cpu_pct` — the raw INPUTS — and none asserted the SCORE derived from them |
| `cluster.scores` never published | "decisions are scored" matched a **log line**; the topic was empty |
| `decision_history.json` never written | `wc -l` on a file seeded with `[]` is 1 line |
| `applying` never published | the check was NAMED for three states and asserted two |
| isolate judged failed forever | the mismatch was printed as incidental detail in a passing check |
| `validate_action` skipped | nothing asserted the live policy ran the same guards as the simulator |

**Fixes applied beyond the defects themselves:**

- **gate4 6b/6c** — health must FALL under a leak and status must degrade. This
  is the assertion whose absence let a constant survive eight phases.
- **gate7 8b/8c** — scores must reach `cluster.scores` with all four components,
  not merely appear in a log.
- **gate6 13** — count transition RECORDS and require `applying` and `completed`
  among their states, replacing `wc -l > 0`.
- **gate6 2** — assert the brain's PROVENANCE (`rule_based.__file__` etc. under
  `/simulator/`), not the absence of a traceback. A local reimplementation would
  also start cleanly and log the right banner, voiding the thesis while every
  check stayed green.
- **gate3 2** — "writes are committing" tested the load generator's `ok` counter,
  which counts reads too. It now requires `commit_index` to ADVANCE, which only
  a committed Raft write can do.

**Rule for any future gate:** assert the property, not a signal correlated with
it. If the check would still pass when the feature is deleted, it is not a check.

## 8.5 MEASURED — a gate can race the system it is testing (2026-10-13)

Gate 4's two new health assertions failed, and the obvious reading — "the
health score is still broken" — was wrong. A controlled cold-start measurement
showed the opposite:

```
LEAK rss=117.4  baseline=44.62  health= 87.9  healthy
LEAK rss=189.7  baseline=44.62  health= 75.8  healthy
LEAK rss=262.0  baseline=44.62  health= 63.8  degraded    <- works
LEAK rss= 44.2  baseline=None   health=100.0  healthy     <- node RESTARTED
```

The score works. What follows it is the agent diagnosing the leak and the
executor restarting the node — the full pipeline doing its job. The baseline
resets to `None` because that is a brand-new process (§5.5 re-warm on
`boot_id` change).

**Gate 4 was measuring across its own recovery.** It captured peak RSS *before*
the repair and sampled health *after*, when the node was freshly booted and
healthy again. The same root cause broke its crash check: `docker kill` no
longer means "stays dead", because the executor brings it back — so the Phase 4
assertion `victim not in after` now fails for the best possible reason.

**The structural point: gate 4 is a Phase 4 gate running in a Phase 8 world.**
It was written when nothing recovered anything, and its assumptions about fault
persistence silently stopped holding the moment Phase 6 landed. Every phase
gate inherits assumptions about what the system does NOT yet do, and those
expire without warning.

Corrections: both checks now scan the topic from the beginning for evidence
that the condition *ever* held, rather than sampling current state. The crash
check accepts either outcome — still gone, or back under a NEW `boot_id` — and
treats only "never went away at all" as a failure.

**Rule, alongside §8.4's:** an assertion about a fault persisting has a shelf
life. When a later phase adds the capability to repair that fault, re-read
every earlier gate for assumptions the repair invalidates.

### 8.5a MEASURED — a baseline built inside a fault is poisoned (2026-10-17)

Gate 5 kept one false positive after the suppression window was widened, and
the evidence strings said why:

```
...653948  latency_spike node-3 | rtt to 3/3 peers elevated
...676981  latency_spike node-3 | rtt to 4/4 peers elevated
```

The peer count grew 3 -> 4 mid-injection: node-4, crashed earlier in the same
run, had been restarted by the executor and rejoined. It accumulated its 20
RTT samples quickly — but every one of them was taken DURING the latency
fault, so its "baseline" was the degraded value and it never read as elevated.
That broke the all-peers-elevated rule (§5.2c), the diagnosis lapsed, the
incident closed, and a second one opened for the same fault.

The fix is a **minimum history span** (60 s), not merely a sample count: a peer
whose RTT history does not predate the fault cannot supply a trustworthy
baseline, and is excluded from `measured` until it can.

This is the same class as §5.5's warm-up and §5.2a's leadership-regime filter —
**a measurement is only as good as the period its reference was drawn from** —
and it is the third place that principle has had to be applied. Worth stating
once, generally: any baseline in this system must be qualified by *when* it was
taken, not just by how many samples it has.

## 9. Tests

Docker-free, fixture-driven, high value per line. Record `cluster.health` JSONL during
Phases 3–4 and replay:

| Test | Asserts |
|---|---|
| `test_classifier.py` | each fixture → expected `FailureDiagnosis`; ambiguous stays ambiguous; missing peer row ≠ unreachable; **a 1-sample reachability blip emits nothing**; 3 consecutive do; 1 good sample clears |
| `test_adapter.py` | `.nodes` shape; `get_stats()` returns percent **strings**; stale node → `failed`; `replica_lag` tracks `apply_lag` and never `match_lag` |
| `test_split_brain.py` | same-term dual leadership → violation; cross-term → `stale_leader_window_ms`, not a violation |
| `test_idempotency.py` | duplicate `decision_id` rejected; superseded on later `decided_at`; orphaned `applying` → `reconciled`, never re-applied |
| `test_action_aware.py` | **one case per row of §6.2a.** Critically: `boot_id` change → `already_satisfied` for `restart_node` but **`stale` for `reroute_traffic`, `isolate_node`, `load_shed`**; unreachable target **does not block** `reroute_traffic`/`rebalance_load`; unreachable → `already_satisfied` for `isolate_node`; unreachable → `proceed` for `restart_node` |
| `test_transition_log.py` | folding an append-only log yields latest-state; a rewritten line is never produced; crash mid-`applying` is detected on fold |
| `test_warmup.py` | no diagnosis in the first 60 s; `boot_id` change re-enters warm-up so post-restart RSS climb emits nothing (the oscillation guard); p99 with <20 requests abstains rather than firing `latency_spike` |
| `test_evaluator.py` | **optimal assignment beats greedy on an overlapping-incident fixture** (the order-dependence regression); **a spurious diagnosis with no incident stays a false positive rather than being force-matched** (the dummy-assignment regression); an incident with no diagnosis stays *missed*; cascade neighbour → *wrong target*; window uses `resolved_at`, never `duration_hint`; latency over true positives only; all four constants surfaced in the report |

Skip integration tests — the phase gates cover that.

---

## 10. Action reality matrix

| Action | Real? | Mechanism | Phase |
|---|---|---|---|
| `restart_node` | yes | `docker restart` / `ENSURE_RUNNING` | 6 |
| `isolate_node` | yes | `AdminAction(ISOLATE)` + iptables | 6 |
| `monitor_only` | yes | no-op | 6 |
| `reroute_traffic` | yes | `SetWeights` | 8 |
| `rebalance_load` | yes | flatten weights | 8 |
| `load_shed` / `readmit` | yes | reject new requests + weight 0 | 8 |
| `promote_replica` | **partial** | forced step-down via timed isolation — triggers a real election, winner not chosen | 8 |
| `reassign_role` | **no** | no referent under Raft | dropped |

---

## 11. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Executor mounts `/var/run/docker.sock` | **root-equivalent host control** | Localhost only. Label the executor as host-level control in the README and say so when demoing. |
| Control plane is not HA | agent, executor, and single-broker Kafka are all SPOFs; module globals make the agent unreplicable by construction | Name it first: the thesis is decision quality, not control-plane availability. Do not imply the system is fault tolerant. |
| Partition detection easier than reality | overstates diagnosis capability | Documented in §5.6; optional `--blackout` row |
| `iptables`/`tc` need `NET_ADMIN` | injection silently no-ops | Assert the capability at injector startup, fail loudly |
| Kafka KRaft needs fixed `CLUSTER_ID` | broker won't start | Generate once, hardcode in the template |
| LLM latency ~1-3 s vs 1 Hz telemetry | decisions act on stale state | Async decisions; cooldown ([llm_agent.py:698](llm_agent.py#L698)); executor staleness rejection (§6.2) |
| Leaked memory OOM-kills a container | looks like a crash, muddies attribution | Per-node `mem_limit`, capped leak rate; the resulting kill is still a legitimate real failure |
| Raft won't force a leader (confirmed, 0.2) | `promote_replica` can trigger an election but not pick the winner | Forced step-down; log intent vs actual outcome separately. Frame honestly — this is correct Raft behavior, not a limitation of the build. |
| Forced step-down costs real availability | isolating the leader creates a genuine unavailability window | Cap at ~2× `raftMaxTimeout`; only reachable via tier-2/3, never a tier-1 rule |

---

## 12. Build order summary

| Phase | Deliverable | Gate |
|---|---|---|
| 0 | Leadership API answer, voter split, versions | both gates answered in writing |
| 1 | Compose + Kafka + nodes + heartbeat | N heartbeats at N=3 and N=5 |
| 2 | Real Raft | kill leader → real election |
| 3 | Load generator + `req_*` | weight change moves real load |
| 4 | Failure injector + `rss_*`, `peers_reachable` | 5 types degrade health; partition shows asymmetric reachability |
| 5 | Diagnosis layer, read-only | **baseline accuracy + detection latency recorded** |
| 6 | Policy (rules) + executor | **crash auto-recovers unattended ★ FREEZE** |
| 7 | LLM + scorer + predictor + memory | tier-2 decision scored and persisted |
| 8 | Full action set | 5-type smoke test green |
| 9 | Demo polish | optional |
