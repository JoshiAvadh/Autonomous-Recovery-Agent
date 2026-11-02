#!/usr/bin/env python3
"""Render docker-compose.yml for an N-node live cluster. plan.md §0.3, Phase 1.

Docker Compose `--scale` does not fit here: Raft needs stable, known peer
identities, not fungible replicas. Node count is therefore resolved at RENDER
time, emitting explicit node-1..node-N services with static hostnames.

Tier split (plan.md §0.3):
    node-1..3   voters   -> Primary / Replica   (never sheddable)
    node-4..N   readonly -> Worker / Gateway    (sheddable)

The split exists because _rank_shed_candidates() (llm_agent.py:326) refuses to
shed any Primary or Replica. If every node were a voter, load shedding could
never fire at all.

Runs on the host, so it stays Python 3.9-compatible and dependency-free.
"""

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "docker-compose.template.yml")
OUTPUT = os.path.join(HERE, "docker-compose.yml")

# Fixed so an ephemeral broker formats identically on every `up`. A random id
# per run would be fine today but breaks the moment a volume is added.
CLUSTER_ID = "5L6g3nShT-eMCtK--X86sw"

VOTER_COUNT = 3
GRPC_PORT = 8000          # inside the container; host port is 8000 + i
RAFT_PORT = 9000          # inside the container; never published
HOST_PORT_BASE = 8000
LOADGEN_PORT = 9500
AGENT_STATUS_PORT = 9600
DASHBOARD_PORT = 9700
# Configurable for demo runs ONLY. The Phase 5 control run and the recorded
# baseline must use this default — lowering it reintroduces exactly the
# startup false positives the rule exists to prevent (plan.md §7 Phase 6).
WARMUP_SECONDS = 60
MEM_LIMIT = "512m"        # bounds the Phase 4 memory leak (plan.md §11)


def role_for(i):
    """Mirror cluster_config.generate_cluster() (cluster_config.py:16-19) so the
    live cluster and the simulator describe the same shape."""
    if i == 1:
        return "Primary"
    if i <= 3:
        return "Replica"
    return "Worker" if i % 2 == 0 else "Gateway"


def raft_mode_for(i):
    return "voter" if i <= VOTER_COUNT else "readonly"


def node_service_block(i, peers, all_nodes=""):
    node_id = "node-%d" % i
    role = role_for(i)
    mode = raft_mode_for(i)
    host_port = HOST_PORT_BASE + i

    return """  {nid}:
    build:
      context: .
      dockerfile: node/Dockerfile
    image: live_cluster/node:latest
    container_name: {nid}
    hostname: {nid}
    labels:
      live_cluster.role: "node"
      live_cluster.raft_mode: "{mode}"
      live_cluster.node_role: "{role}"
    environment:
      NODE_ID: "{nid}"
      NODE_ROLE: "{role}"
      RAFT_MODE: "{mode}"
      NODE_PEERS: "{peers}"
      ALL_NODES: "{all_nodes}"
      GRPC_PORT: "{grpc}"
      RAFT_PORT: "{raft}"
      KAFKA_BOOTSTRAP: "kafka:9092"
      HEALTH_INTERVAL: "1.0"
      LOG_LEVEL: "INFO"
    ports:
      - "{host_port}:{grpc}"
    cap_add:
      - NET_ADMIN
    mem_limit: {mem}
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - live_cluster
    # restart: "no" is REQUIRED, not a default. A restart policy would have
    # Docker resurrect a killed container on its own, so `inject.py crash`
    # would leave nothing for the recovery system to detect or fix — the
    # Phase 6 gate would pass without the agent doing anything at all.
    restart: "no"
""".format(
        nid=node_id,
        role=role,
        mode=mode,
        peers=peers,
        all_nodes=all_nodes,
        grpc=GRPC_PORT,
        raft=RAFT_PORT,
        host_port=host_port,
        mem=MEM_LIMIT,
    )


def loadgen_service_block(n):
    nodes = ",".join("node-%d" % i for i in range(1, n + 1))
    deps = "\n".join("      %s:\n        condition: service_started" % ("node-%d" % i)
                     for i in range(1, n + 1))
    return """  load_generator:
    build:
      context: .
      dockerfile: load_generator/Dockerfile
    image: live_cluster/loadgen:latest
    container_name: load_generator
    hostname: load_generator
    labels:
      live_cluster.role: "loadgen"
    environment:
      LOADGEN_NODES: "{nodes}"
      NODE_GRPC_PORT: "{grpc}"
      LOADGEN_PORT: "{port}"
      TARGET_RPS: "20"
      WRITE_PCT: "0.2"
      LOADGEN_WORKERS: "8"
      LOG_LEVEL: "INFO"
    ports:
      - "{port}:{port}"
    depends_on:
{deps}
    networks:
      - live_cluster
    restart: "no"
""".format(nodes=nodes, grpc=GRPC_PORT, port=LOADGEN_PORT, deps=deps)


def agent_service_block():
    return """  recovery_agent:
    build:
      context: .
      dockerfile: recovery_agent/Dockerfile
    image: live_cluster/agent:latest
    container_name: recovery_agent
    hostname: recovery_agent
    labels:
      live_cluster.role: "agent"
    environment:
      KAFKA_BOOTSTRAP: "kafka:9092"
      AGENT_GROUP_ID: "recovery-agent"
      AGENT_STATUS_PORT: "{status}"
      WARMUP_SECONDS: "{warmup}"
      # Read from live_cluster/.env by compose's own substitution. The agent
      # needs it for the tier 2/3 LLM path; config.py picks it up via
      # os.getenv, so no code change is required.
      GROQ_API_KEY: "${{GROQ_API_KEY:-}}"
      LLM_MODEL: "${{LLM_MODEL:-openai/gpt-oss-120b}}"
      LLM_MAX_TOKENS: "${{LLM_MAX_TOKENS:-2000}}"
      DIAGNOSE_EVERY_S: "1.0"
      LOG_LEVEL: "INFO"
    ports:
      - "{status}:{status}"
    volumes:
      # The reused decision brain, shared with the simulator rather than
      # copied, so both run the same files and share decision_history.json.
      # MUST be :rw, not :ro. decision_memory.save() needs to write
      # decision_history.json here, and it swallows the IOError with a warning
      # (decision_memory.py:42) — so a read-only mount makes cross-run
      # learning silently write nothing while appearing to work.
      #
      # A nested single-file :rw bind over a :ro parent was tried first and
      # DOES NOT WORK on Docker Desktop for macOS: `docker inspect` reports
      # rw=true on the inner mount, but the write still fails EROFS because
      # the parent read-only bind makes the underlying filesystem read-only at
      # the VM layer. Verified, not assumed.
      - ..:/simulator:rw
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - live_cluster
    restart: "no"
""".format(status=AGENT_STATUS_PORT, warmup=WARMUP_SECONDS)


def executor_service_block():
    return """  executor:
    build:
      context: .
      dockerfile: executor/Dockerfile
    image: live_cluster/executor:latest
    container_name: executor
    hostname: executor
    labels:
      live_cluster.role: "executor"
    environment:
      KAFKA_BOOTSTRAP: "kafka:9092"
      EXECUTOR_GROUP_ID: "executor"
      NODE_GRPC_PORT: "{grpc}"
      LOADGEN_HOST: "load_generator"
      LOADGEN_PORT: "{lg}"
      EXECUTOR_STATE: "/state/transitions.jsonl"
      LOG_LEVEL: "INFO"
    volumes:
      # ROOT-EQUIVALENT HOST CONTROL (plan.md §11). The executor needs the
      # Docker API to restart containers and to exec iptables into them.
      # Localhost development only — never expose this stack beyond it.
      - /var/run/docker.sock:/var/run/docker.sock
      # Write-ahead transition log must outlive the container, or the §6.3
      # crash-recovery guarantee is void.
      - executor_state:/state
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - live_cluster
    restart: "no"
""".format(grpc=GRPC_PORT, lg=LOADGEN_PORT)


def dashboard_service_block():
    return """  dashboard:
    build:
      context: .
      dockerfile: dashboard/Dockerfile
    image: live_cluster/dashboard:latest
    container_name: dashboard
    hostname: dashboard
    labels:
      live_cluster.role: "dashboard"
    environment:
      KAFKA_BOOTSTRAP: "kafka:9092"
      DASHBOARD_PORT: "{port}"
      LOG_LEVEL: "INFO"
    ports:
      - "{port}:{port}"
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - live_cluster
    restart: "no"
""".format(port=DASHBOARD_PORT)


def render(n):
    if n < VOTER_COUNT:
        raise SystemExit(
            "--nodes must be >= %d: the Raft group needs %d voters (plan.md §0.3)"
            % (VOTER_COUNT, VOTER_COUNT)
        )

    # Every voter address, including the node's own. Each node subtracts itself
    # to form pysyncobj's partner list, so one uniform string serves all nodes.
    peers = ",".join("node-%d:%d" % (i, RAFT_PORT) for i in range(1, VOTER_COUNT + 1))

    with open(TEMPLATE, "r") as fh:
        template = fh.read()

    all_nodes = ",".join("node-%d" % i for i in range(1, n + 1))
    blocks = "\n".join(node_service_block(i, peers, all_nodes) for i in range(1, n + 1))
    blocks += "\n" + loadgen_service_block(n)
    blocks += "\n" + agent_service_block()
    blocks += "\n" + executor_service_block()
    blocks += "\n" + dashboard_service_block()
    out = template.replace("__NODE_SERVICES__", blocks)
    out = out.replace("__CLUSTER_ID__", CLUSTER_ID)

    with open(OUTPUT, "w") as fh:
        fh.write(out)

    return peers


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--nodes", type=int, default=3,
                    help="total nodes (>=3); the first 3 are Raft voters")
    args = ap.parse_args()

    peers = render(args.nodes)

    voters = min(args.nodes, VOTER_COUNT)
    readonly = max(0, args.nodes - VOTER_COUNT)
    print("rendered %s" % os.path.relpath(OUTPUT, HERE))
    print("  %d nodes: %d voter(s), %d readonly" % (args.nodes, voters, readonly))
    for i in range(1, args.nodes + 1):
        print("    node-%-2d %-8s %-8s  grpc -> localhost:%d"
              % (i, role_for(i), raft_mode_for(i), HOST_PORT_BASE + i))
    print("  raft peers: %s" % peers)
    print("\nnext: docker compose up -d --build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
