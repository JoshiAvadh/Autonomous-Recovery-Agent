# Hybrid Recovery in Distributed Systems

A SimPy-based discrete-event simulator that combines rule-based and LLM-driven failure recovery to build self-healing distributed clusters.

## Overview

This project simulates a distributed system (5, 10, or 20 nodes) with realistic consensus, request-level communication, and five types of injected failures. It compares two recovery strategies:

- **Rule Engine** — fast, deterministic, handles known patterns instantly
- **LLM Agent (Groq Llama 3.3-70B)** — contextual reasoning for complex, overlapping failures

Decisions are routed through a 3-tier confidence system: simple failures go to rules, ambiguous ones go to the LLM. Every decision is scored on speed, stability, cascade prevention, and message delivery.

## Architecture

```
Cluster Layer        5/10/20 nodes (Primary, Replica, Worker, Gateway)
       |             Consensus protocol, heartbeats, request simulation
       v
Failure Injection    Crash, Partition, Latency Spike, Memory Leak, CPU Overload
       |             7 escalating phases from warmup to multi-failure chaos
       v
Decision Engine      Rule-based confidence scoring + 3-tier routing
       |             LLM chain-of-thought + multi-turn verification
       v
Recovery Layer       Action execution, feedback loop (t+5), load shedding
       |             Predictive detection, cross-run learning
       v
Scoring System       Speed (35%) + Stability (25%) + Cascade (25%) + Delivery (15%)
```

## Key Features

- **Consensus-aware simulation** — quorum writes, leader election, replica lag, split-brain prevention, commit index tracking
- **Request-level model** — individual read/write requests with queuing, retries (exponential backoff), timeouts, and packet loss
- **Tiered decision routing** — Tier 1 (>=75% confidence): rules handle, Tier 2 (45-74%): LLM decides, Tier 3 (<45%): LLM emergency
- **4-component scoring** — every decision scored 5 ticks after execution on MTTR, health delta, cascade count, and delivery rate
- **Load shedding** — when timeout rate >50%, non-consensus nodes are shed to restore service
- **Predictive detection** — linear regression on health history predicts failures before they happen
- **Feedback loop** — failed actions trigger LLM follow-up with cluster snapshot and failure reason
- **Cross-run learning** — decision outcomes persisted to `decision_history.json`, top 5 past results injected into future LLM prompts

## Failure Types

| Type | Mechanism | Duration |
|------|-----------|----------|
| Crash | Node offline, health to 0%, recovers at 40% | 3-8 ticks |
| Network Partition | Bidirectional link cut between two nodes | 3-7 ticks |
| Latency Spike | Response times multiply 2-5x, triggers retries | 2-5 ticks |
| Memory Leak | Health drains 3-8%/tick, early alert at 50% | Until intervention |
| CPU Overload | Load spikes 30-60%, slows processing | 3-7 ticks |

## Metrics Reported

- MTTR (Mean Time to Recovery)
- Failure resolution rate
- Message delivery rate
- Write commit success rate
- Request timeout rate
- Stale read rate
- Quorum unavailable time
- Leader elections and split-brain events
- LLM vs Rule decision quality comparison

## Project Structure

```
main.py              Simulation loop — event processing, failure handling, scoring
network.py           Cluster model — nodes, consensus, requests, queues, metrics
failures.py          Fault injector — 5 failure types, 7-phase scenario
rule_based.py        Rule engine — confidence scoring, tiered routing, actions
llm_agent.py         LLM agent — Groq integration, chain-of-thought, load shedding
scorer.py            Decision scoring — 4-component weighted system
predictor.py         Health prediction — linear regression, alert suppression
feedback.py          Feedback loop — success checks at t+5, follow-up actions
decision_memory.py   Cross-run learning — persistent history, context injection
event_bus.py         Thread-safe event queue for failure propagation
config.py            Environment config — API keys, feature flags, thresholds
cluster_config.py    Dynamic cluster generation and failure scheduling
logger.py            Structured logging with cluster snapshots
app.py               Flask web dashboard for running simulations
benchmark.py         Multi-size, multi-seed automated testing
compare.py           LLM vs rule-only comparison mode
generate_charts.py   Chart generation for 5/10-node results
generate_charts_20.py Chart generation for 20-node results
```

## Setup

```bash
pip install simpy groq python-dotenv python-pptx matplotlib pillow flask
```

Create a `.env` file:
```
GROQ_API_KEY=your_api_key_here
```

## Usage

Run a single simulation:
```bash
python main.py
```

Run benchmarks across cluster sizes:
```bash
python benchmark.py 5,10 3
```

Generate charts:
```bash
python generate_charts.py
python generate_charts_20.py
```

Generate presentation:
```bash
python generate_ppt.py
```

## Results

Across simulation runs, the hybrid system demonstrates:

- **LLM outperforms rules by 12-23 points** on complex multi-failure scenarios
- **Memory leak MTTR reduced from ~17 ticks to ~5 ticks** through early intervention
- **100% commit rate** and **0% timeout rate** achieved in later runs
- **Cross-run learning converges** — LLM stops choosing low-scoring actions after seeing outcomes
- Rules remain effective for simple single-failure cases (comparable scores, zero API cost)
