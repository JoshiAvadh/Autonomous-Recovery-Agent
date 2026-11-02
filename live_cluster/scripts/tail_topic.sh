#!/usr/bin/env bash
# Tail a cluster topic. Usage: ./scripts/tail_topic.sh cluster.health [--from-start]
#
#   ./scripts/tail_topic.sh cluster.health            # live tail
#   ./scripts/tail_topic.sh cluster.health --count 10 # first 10 then exit
set -euo pipefail

TOPIC="${1:-cluster.health}"
shift || true

FROM_BEGINNING=""
MAX_MESSAGES=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-start) FROM_BEGINNING="--from-beginning"; shift ;;
    --count)      MAX_MESSAGES="--max-messages $2"; FROM_BEGINNING="--from-beginning"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

echo "--- tailing ${TOPIC} (ctrl-c to stop) ---" >&2

# shellcheck disable=SC2086
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "${TOPIC}" \
  --property print.key=true \
  --property key.separator=' | ' \
  ${FROM_BEGINNING} ${MAX_MESSAGES}
