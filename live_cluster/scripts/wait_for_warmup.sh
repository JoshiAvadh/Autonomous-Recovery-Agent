#!/usr/bin/env bash
# Block until every node clears §5.5 warm-up. REQUIRED before any injection.
#
# The classifier suppresses all diagnoses for a node's first 60s, so a script
# that injects right after `docker compose up` gets silence — and the gate
# looks like it hung or like the classifier missed the incident, when in fact
# the system behaved correctly. This makes the wait visible and intentional
# rather than letting it masquerade as a failure.
set -euo pipefail

cd "$(dirname "$0")/.."

AGENT_PORT="${AGENT_STATUS_PORT:-9600}"
TIMEOUT="${WARMUP_WAIT_TIMEOUT:-180}"

deadline=$(( $(date +%s) + TIMEOUT ))
last=""

while [[ $(date +%s) -lt $deadline ]]; do
  body="$(docker compose exec -T recovery_agent \
            python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:${AGENT_PORT}/warmup').read().decode())" \
          2>/dev/null || true)"

  if [[ -n "$body" ]]; then
    if python3 -c "import json,sys; sys.exit(0 if json.loads(sys.argv[1]).get('all_warm') else 1)" "$body" 2>/dev/null; then
      echo "all nodes warm — injecting"
      exit 0
    fi
    pretty="$(python3 - "$body" <<'PY' 2>/dev/null || true
import json, sys
d = json.loads(sys.argv[1])
parts = []
for node, left in sorted(d.get("remaining", {}).items()):
    parts.append("%s %s" % (node, "OK" if left <= 0 else "%.0fs" % left))
print("  ".join(parts))
PY
)"
    if [[ "$pretty" != "$last" ]]; then
      echo "waiting for warm-up: ${pretty}"
      last="$pretty"
    fi
  fi
  sleep 3
done

echo "warm-up did not complete within ${TIMEOUT}s" >&2
exit 1
