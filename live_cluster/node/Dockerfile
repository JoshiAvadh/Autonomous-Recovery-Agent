FROM python:3.11-slim

# iproute2 -> tc netem (latency injection, Phase 4)
# iptables  -> DROP rules (partition + isolate, Phase 4/6)
# Both run INSIDE this container via docker exec, so they must be present here
# and the container needs cap_add: NET_ADMIN (set in the compose template).
RUN apt-get update && apt-get install -y --no-install-recommends \
        iproute2 \
        iptables \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Generate gRPC stubs at build time into /app (flat), because the generated
# node_pb2_grpc.py does a bare `import node_pb2`.
COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
        -I/app/proto \
        --python_out=/app \
        --grpc_python_out=/app \
        /app/proto/node.proto

COPY common/ /app/common/
COPY node/ /app/node/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

CMD ["python", "/app/node/node_service.py"]
