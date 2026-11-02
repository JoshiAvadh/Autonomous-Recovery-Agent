# live_cluster

A real, networked distributed cluster that reuses the SimPy simulator's decision
brain against actual containers, real Raft consensus, and a real Kafka backbone.

**Thesis:** can a recovery policy developed in simulation make correct, measurable
decisions against a real system — when it must diagnose the failure itself, with
no ground-truth labels?

Full design, phase gates, and contracts: [`../plan.md`](../plan.md).

## Status

**Phase 1 complete** — Kafka (KRaft) + N nodes + 1 Hz heartbeat. No Raft yet
(Phase 2), no traffic (Phase 3), no failures (Phase 4), no agent (Phase 5+).

## Quick start

```bash
python3 generate_compose.py --nodes 5
docker compose up -d --build
./scripts/tail_topic.sh cluster.health
```

Run the phase gate:

```bash
./scripts/gate1.sh
```

Tear down with `docker compose down`.

## Layout

```
proto/node.proto        full gRPC contract (plan.md §4.3)
common/schemas.py       every topic's message shape (plan.md §4)
common/kafka_io.py      JSON producer / consumer helpers
node/node_service.py    gRPC server + health publisher
generate_compose.py     renders docker-compose.yml for N nodes
scripts/gate1.sh        Phase 1 gate
scripts/tail_topic.sh   tail any cluster topic
```

## Three things that are load-bearing, not incidental

**`restart: "no"` on every node.** A Docker restart policy would resurrect a
killed container on its own, so `inject.py crash` would leave nothing for the
recovery system to detect or repair — later gates would pass without the agent
doing anything at all.

**`boot_id` is the only restart proof.** `uptime` is a duration: it says *how
long*, never *which process*. Every restart assertion and staleness check keys
on `boot_id` (uuid4, generated once per process) with `started_at` for ordering.
See plan.md §4.1a.

**The agent never subscribes to `cluster.failures`.** Ground truth is
evaluator-only. Feeding it to the agent would collapse the thesis into "given a
labeled failure, pick an action" — which the simulator already answers.
`common/schemas.AGENT_INPUT_TOPICS` encodes the restriction.

## Security

From Phase 6 the executor mounts `/var/run/docker.sock`, which is
**root-equivalent control of the host**. This stack is for localhost
development only and must not be exposed beyond it. The control plane is also
deliberately not highly available — single-broker Kafka, one agent, one
executor — because the thesis is decision quality, not control-plane
availability.
