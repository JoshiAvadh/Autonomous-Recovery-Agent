FROM python:3.11-slim

# iptables for isolate_node's DROP rules, applied into target containers via
# the Docker exec API.
RUN apt-get update && apt-get install -y --no-install-recommends iptables \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
        -I/app/proto \
        --python_out=/app \
        --grpc_python_out=/app \
        /app/proto/node.proto /app/proto/loadgen.proto

COPY common/ /app/common/
COPY executor/ /app/executor/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    EXECUTOR_STATE=/state/transitions.jsonl

CMD ["python", "/app/executor/executor_service.py"]
