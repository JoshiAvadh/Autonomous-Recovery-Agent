# Autonomous Recovery Agent

An LLM-driven self-healing agent for distributed clusters. It watches live telemetry, works out what has failed **without being told**, decides how to recover, and carries out the fix against real containers: real Raft consensus, a real Kafka backbone, real `docker restart`s, real `iptables` partitions.

> **The question this project answers:** can a recovery policy developed in simulation make correct, measurable decisions against a real distributed system, when it has to diagnose the failure itself with no ground-truth labels?

```
real telemetry ──► diagnosis ──► hybrid policy (rules + LLM) ──► real async action ──► independent evaluation
```

The project has two halves that share one decision brain:

| | What it is | Where |
|---|---|---|
| **Simulation lab** | A SimPy discrete-event model of a 5/10/20-node cluster, used to develop and benchmark the recovery policy cheaply and reproducibly | repo root |
| **Live cluster** | A real, containerized cluster (Docker + Raft + Kafka) where the **same, unmodified** policy files run against real failures | [`live_cluster/`](live_cluster/) |

The live agent imports `rule_based.py`, `llm_agent.py`, `scorer.py`, `predictor.py` and `decision_memory.py` straight from the repo root. Nothing is copied or forked, so the system tested in simulation is the system running live.

---

## How it works

### 1. Diagnose: no labels allowed
Every node publishes health at 1 Hz to `cluster.health`. The agent's classifier reads only that stream and produces a `FailureDiagnosis` (type, target, confidence, evidence) over a rolling window.

The failure injector publishes ground truth to `cluster.failures`, and **the agent never subscribes to it**. If the agent could see labels, the project would reduce to "given a labeled failure, pick an action", which the simulator already answers. Ground truth is used only by the offline evaluator.

### 2. Decide: tiered hybrid routing
| Confidence | Who decides | Why |
|---|---|---|
| **≥ 75%** (Tier 1) | Rule engine | Fast, deterministic, zero API cost |
| **45–74%** (Tier 2) | LLM agent (Groq · Llama 3.3-70B) | Contextual reasoning for overlapping or ambiguous failures |
| **< 45%** (Tier 3) | LLM emergency path | Lowest confidence, highest stakes |

The LLM uses chain-of-thought with multi-turn verification. It also gets the outcomes of past decisions that scored well, injected from `decision_history.json` (cross-run learning).

### 3. Act: a real executor
Decisions are published as intents to `cluster.actions`. The executor validates each one against a fresh state read (rejecting stale or unsafe actions), applies it, and reports `accepted → completed` to `cluster.action_results`. A write-ahead transition log keeps it durable across restarts.

| Action | Mechanism |
|---|---|
| `restart_node` | `docker restart` / `ENSURE_RUNNING` (proven by a new `boot_id`) |
| `isolate_node` | gRPC `AdminAction(ISOLATE)` + iptables |
| `reroute_traffic` / `rebalance_load` | Load-generator `SetWeights` |
| `load_shed` / `readmit` | Reject new client requests but **keep Raft participation** |
| `promote_replica` | Forced leader step-down → a real election (Raft picks the winner, not us) |
| `monitor_only` | No-op |

### 4. Score and learn
Every decision is scored a few seconds after it runs on **speed (35%) + stability (25%) + cascade prevention (25%) + delivery (15%)**. A feedback loop checks whether the action actually worked and triggers an LLM follow-up if it didn't. Scored outcomes feed back into future prompts.

---

## Live cluster architecture

```
  failure_injector ──► ┌──────────── Docker network ────────────┐
  (crash, partition,   │  node-1  node-2  node-3   ◄─ Raft voters│
   latency, leak, cpu) │  node-4  node-5 ...       ◄─ sheddable  │
  load_generator ────► └──────────────┬─────────────────────────┘
                                      │ health @ 1 Hz
                                      ▼
            ┌───────────────── Kafka (KRaft) ──────────────────┐
            │ cluster.health  cluster.diagnoses  cluster.actions│
            │ cluster.action_results  cluster.scores            │
            │ cluster.failures  ◄── ground truth (evaluator only)│
            └──────┬────────────────────────────────────▲───────┘
                   ▼                                    │
         ┌──────────────────────┐   intents   ┌─────────┴──────┐
         │   RECOVERY AGENT     │────────────►│    EXECUTOR    │──► Docker / gRPC
         │ classify → rules/LLM │             │ validate→apply │
         │ scorer · predictor   │             └────────────────┘
         │ decision memory      │
         └──────────────────────┘
   dashboard (localhost:9700)  ·  offline evaluator (optimal matching vs ground truth)
```

## Failure types

| Failure | Injected for real by | Simulated as |
|---|---|---|
| Crash | `docker kill` | Node offline, health → 0% |
| Network partition | `iptables` link cut between two nodes | Bidirectional link cut |
| Latency spike | `tc netem` delay on one node | Response times ×2–5 |
| Memory leak | Growing RSS inside the container (bounded by `mem_limit`) | Health drains 3–8%/tick |
| CPU overload | Busy-loop workers | Load spikes 30–60% |

## Status

All live-cluster phases are complete, with every gate green:

| Phase | Gate result |
|---|---|
| Real Raft, load generator, failure injector | G3 8/8 · G4 14/14 |
| Diagnosis layer, read-only | G5 12/12, **4/4 recall, 0 false positives** |
| Rules + executor: crash auto-recovers unattended | G6 13/13 ★ |
| LLM tiers, scorer, predictor, memory | G7 14/14 |
| Full action set | G8 18/18 |
| Live dashboard | built |

Fixture suites: 22 classifier cases and 15 action-aware validation cases. The design, contracts, measurements and gate audits are written up in [`plan.md`](plan.md).

### Simulation results
In the simulation lab, according to the project's benchmark runs:
- The **LLM outperforms rules by 12–23 points** on complex multi-failure scenarios
- **Memory-leak MTTR drops from ~17 ticks to ~5** through early intervention
- **Cross-run learning converges**: the LLM stops choosing low-scoring actions after seeing their outcomes
- **Rules stay competitive** on simple single-failure cases, at zero API cost

Absolute scores are not directly comparable between simulation and the live cluster: time is ticks in one and seconds in the other, and the telemetry windows differ. See `plan.md` §8.3.

---

## Quick start

### Prerequisites
- Python 3.9+
- Docker with Compose v2 (for the live cluster)
- A [Groq API key](https://console.groq.com/) (for the LLM tiers)

Create a `.env` file in the repo root, and also in `live_cluster/` for the live agent:
```
GROQ_API_KEY=your_api_key_here
```

### Live cluster
```bash
cd live_cluster
python3 generate_compose.py --nodes 5      # renders docker-compose.yml
docker compose up -d --build
./scripts/wait_for_warmup.sh

# open the dashboard → http://localhost:9700

# break something and watch the agent fix it
python3 failure_injector/inject.py crash     --target node-2
python3 failure_injector/inject.py partition --a node-1 --b node-2 --duration 10
python3 failure_injector/inject.py latency   --target node-3 --ms 300 --duration 10
python3 failure_injector/inject.py leak      --target node-4 --rate-mb 5
python3 failure_injector/inject.py cpu       --target node-5 --workers 2

# score diagnoses against ground truth
python3 evaluator/evaluate.py

docker compose down
```
Run any phase gate with `python3 scripts/gate<N>.py` (gate 1 is `./scripts/gate1.sh`).

### Simulation lab
```bash
pip install simpy groq python-dotenv matplotlib pillow flask

python main.py                       # one simulation run
python app.py                        # web dashboard for running simulations
python benchmark.py 5,10 3           # cluster sizes 5 and 10, 3 seeds each
python compare.py                    # LLM vs rules-only
python generated_charts.py           # charts for 5/10-node results
python generated_charts_20nodes.py   # charts for 20-node results
```

---

## Project structure

```
├── live_cluster/              Real containerized cluster
│   ├── node/                  gRPC node service, Raft status, health, peer probes
│   ├── recovery_agent/        Classifier, incidents, policy bridge, outcome tracking
│   ├── executor/              Validation, action execution, write-ahead transition log
│   ├── failure_injector/      Real chaos: kill, iptables, tc, leak, cpu
│   ├── load_generator/        Weighted client traffic (SetWeights)
│   ├── evaluator/             Offline diagnosis-vs-ground-truth scoring
│   ├── dashboard/             Live node grid + causal timeline (localhost:9700)
│   ├── common/ · proto/       Topic schemas, Kafka helpers, gRPC contracts
│   ├── scripts/               Phase gates 1–8
│   └── tests/                 Classifier and action-aware validation fixtures
│
├── rule_based.py              Rule engine: confidence scoring, tiered routing   ┐
├── llm_agent.py               LLM agent: Groq, chain-of-thought, load shedding   │ shared
├── scorer.py                  4-component decision scoring                       │ decision
├── predictor.py               Health-trend failure prediction                    │ brain
├── decision_memory.py         Cross-run learning                                 ┘
├── feedback.py                Success checks and follow-up actions
├── main.py · network.py       Simulation loop and cluster model (SimPy)
├── failures.py                Simulated fault injector, 7-phase scenario
├── cluster_config.py · config.py · event_bus.py · logger.py
├── app.py · templates/        Simulation web dashboard (Flask)
├── benchmark.py · compare.py  Automated benchmarking
├── generated_charts*.py       Chart generation (output in charts/, charts_20/)
└── plan.md                    Full design doc: contracts, phases, measurements
```

## Honest limits

- **Localhost only.** The executor mounts `/var/run/docker.sock`, which gives root-equivalent control of the host. Never expose this stack beyond your machine.
- **The control plane is not highly available.** Kafka is single-broker, and there is one agent and one executor. The thesis is about decision quality, not control-plane fault tolerance.
- **Raft won't take orders.** `promote_replica` triggers a real election, but the cluster picks the winner. The agent logs the intended and actual outcomes separately.
- **Partition detection is easier here than in production** (`plan.md` §5.6).

## Tech stack
Python · Docker Compose · Apache Kafka (KRaft) · pysyncobj (Raft) · gRPC · Groq (Llama 3.3-70B) · SimPy · Flask · scipy · matplotlib
