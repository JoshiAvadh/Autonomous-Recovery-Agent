#!/usr/bin/env bash
# Phase 1 gate (plan.md §7). Brings the cluster up at N=3 and again at N=5,
# asserting that every node heartbeats and that the voter/readonly tier split
# renders correctly.
#
#   ./scripts/gate1.sh            # full: N=3 then N=5
#   ./scripts/gate1.sh --quick    # N=5 only (~1 min)
set -euo pipefail

cd "$(dirname "$0")/.."

QUICK=0
[[ "${1:-}" == "--quick" ]] && QUICK=1

# `open -a Docker` returns immediately but the daemon needs ~30s. Without this
# the gate dies on "unable to get image" and reads like a broken compose file
# rather than a machine that simply is not ready yet. Same reasoning as the
# Phase 6 warm-up barrier (plan.md §7): make the wait visible and intentional.
wait_for_docker() {
  if docker info >/dev/null 2>&1; then return 0; fi
  echo -n "waiting for docker daemon "
  for _ in $(seq 1 40); do
    if docker info >/dev/null 2>&1; then echo " ready"; return 0; fi
    echo -n "."
    sleep 3
  done
  echo
  echo "docker daemon not reachable after 120s." >&2
  echo "  start Docker Desktop (open -a Docker) and re-run." >&2
  exit 1
}

wait_for_docker

pass=0
fail=0

check() {  # check <label> <expected> <actual>
  if [[ "$2" == "$3" ]]; then
    echo "  PASS  $1 ($3)"
    pass=$((pass + 1))
  else
    echo "  FAIL  $1 — expected $2, got $3"
    fail=$((fail + 1))
  fi
}

run_at_size() {
  local n="$1"
  echo
  echo "=== N=${n} ==============================================="

  docker compose down --remove-orphans >/dev/null 2>&1 || true
  python3 generate_compose.py --nodes "${n}" >/dev/null
  docker compose up -d --build >/dev/null

  echo "  waiting for nodes to publish..."
  sleep 20

  # Collect ~1 sample per node. --count is generous so slow starters still land.
  local sample
  sample=$(./scripts/tail_topic.sh cluster.health --count $((n * 4)) 2>/dev/null || true)

  local distinct
  distinct=$(echo "$sample" | grep -o '"node_id":"node-[0-9]*"' | sort -u | wc -l | tr -d ' ')
  check "distinct node_ids heartbeating" "${n}" "${distinct}"

  local voters
  voters=$(echo "$sample" | grep -o '"role":"\(Primary\|Replica\)"' | wc -l | tr -d ' ')
  if [[ "$voters" -gt 0 ]]; then
    echo "  PASS  voter tier reporting (Primary/Replica samples: ${voters})"
    pass=$((pass + 1))
  else
    echo "  FAIL  voter tier reporting — no Primary/Replica samples"
    fail=$((fail + 1))
  fi

  if [[ "$n" -gt 3 ]]; then
    local ro
    ro=$(echo "$sample" | grep -o '"role":"\(Worker\|Gateway\)"' | wc -l | tr -d ' ')
    if [[ "$ro" -gt 0 ]]; then
      echo "  PASS  readonly tier reporting (Worker/Gateway samples: ${ro})"
      pass=$((pass + 1))
    else
      echo "  FAIL  readonly tier reporting — no Worker/Gateway samples"
      fail=$((fail + 1))
    fi
  fi

  # boot_id must be present and distinct per node: it is the ONLY restart proof
  # the whole execution model rests on (plan.md §4.1a).
  local boots
  boots=$(echo "$sample" | grep -o '"boot_id":"[^"]*"' | sort -u | wc -l | tr -d ' ')
  check "distinct boot_ids" "${n}" "${boots}"
}

if [[ "$QUICK" -eq 0 ]]; then
  # Re-rendering at a different N is half the point of the gate: it proves the
  # generator produces a working cluster at any size, not just the one it was
  # written against. --quick skips that and only checks N=5.
  run_at_size 3
fi
run_at_size 5

echo
echo "=========================================================="
echo "  Phase 1 gate: ${pass} passed, ${fail} failed"
[[ "$QUICK" -eq 1 ]] && echo "  (--quick: N=3 render not exercised)"
echo "=========================================================="
[[ "$fail" -eq 0 ]]
