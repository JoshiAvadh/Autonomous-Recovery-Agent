FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Needs node.proto for the Phase 6 executor-side GetStatus validation; the
# agent itself is read-only and only consumes Kafka.
COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
        -I/app/proto \
        --python_out=/app \
        --grpc_python_out=/app \
        /app/proto/node.proto

COPY common/ /app/common/
COPY recovery_agent/ /app/recovery_agent/

# The reused decision brain (rule_based, llm_agent, scorer, predictor,
# decision_memory, config) is bind-mounted from the repo root at runtime, NOT
# copied — so the live system and the simulator run the exact same files and
# share decision_history.json (plan.md §7 Phase 7).
ENV PYTHONPATH=/app:/simulator \
    PYTHONUNBUFFERED=1

CMD ["python", "/app/recovery_agent/agent_service.py"]
