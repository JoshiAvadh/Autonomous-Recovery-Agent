FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Needs BOTH protos: loadgen for its own control plane, node to drive traffic.
COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
        -I/app/proto \
        --python_out=/app \
        --grpc_python_out=/app \
        /app/proto/node.proto /app/proto/loadgen.proto

COPY common/ /app/common/
COPY load_generator/ /app/load_generator/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

CMD ["python", "/app/load_generator/gen.py"]
