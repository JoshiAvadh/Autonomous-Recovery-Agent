"""Node health score. plan.md §4.5.

THIS WAS MISSING. `health` was hardcoded to 100.0 and `status` to "healthy"
for Phases 1-8, so every consumer of either field read a constant:

  - validate_action's health guards were unreachable (health < 60 impossible)
  - assess_confidence's health penalties never applied, distorting tier routing
  - classifier rule 7 (health < 70 -> ambiguous) could never fire
  - scorer's stability_score always saw an all-healthy cluster
  - _rank_shed_candidates ranks degraded/low-health first — never applied

A node leaking 385 MB over its baseline still reported health=100, status=healthy.
Found via the Phase 7 validator work, not by any gate: no gate asserted that
health MOVES, which is exactly the proxy-vs-property gap that has now produced
six defects.

RSS SCALING DIFFERS FROM §4.5's FIRST DRAFT. That draft capped the RSS penalty
at 30, which floors health at 70 for a pure leak — so the `health < 60` guard in
validate_action would have stayed unreachable even after implementing the score.
A health field that cannot express "this node is in serious trouble from a leak"
fails to drive the reused code's health-keyed logic, which is the entire reason
for reusing it. The cap is 60 and the divisor 6, so a leak approaching the
512 MB container limit drives health toward 40 and crosses every threshold the
simulator's logic expects.
"""

# Replication: a node this far behind is serving badly stale reads.
LAG_WEIGHT = 8.0
LAG_CAP = 40.0

# Request failures in the current 1s window. The cap is 50, not §4.5's draft
# 30: at 30 a node failing 100% of requests scores exactly 70 and reads as
# HEALTHY, because 70 is the degraded boundary. A node rejecting every request
# it receives must never report healthy.
FAILURE_CAP = 50.0

# Memory above baseline. /6 with a 60 cap means ~250 MB over baseline crosses
# health 60, and ~360 MB crosses 40 — inside a 512 MB mem_limit.
RSS_DIVISOR = 6.0
RSS_CAP = 60.0

DEGRADED_BELOW = 70.0
FAILED_BELOW = 20.0

STATUS_HEALTHY = "healthy"
STATUS_DEGRADED = "degraded"
STATUS_FAILED = "failed"


def compute(apply_lag=0, req_ok=0, req_fail=0, req_timeout=0,
            rss_mb=None, rss_baseline_mb=None):
    """Return (health 0-100, status, [reasons]).

    Reasons are returned so the degradation is explainable in telemetry rather
    than arriving as a bare number nobody can account for.
    """
    health = 100.0
    reasons = []

    lag_penalty = min(LAG_CAP, max(0, int(apply_lag)) * LAG_WEIGHT)
    if lag_penalty:
        health -= lag_penalty
        reasons.append("apply_lag=%d (-%.0f)" % (apply_lag, lag_penalty))

    total = (req_ok or 0) + (req_fail or 0) + (req_timeout or 0)
    if total:
        rate = ((req_fail or 0) + (req_timeout or 0)) / float(total)
        fail_penalty = rate * FAILURE_CAP          # linear; rate is 0..1
        if fail_penalty >= 1.0:
            health -= fail_penalty
            reasons.append("fail_rate=%.0f%% (-%.0f)" % (rate * 100, fail_penalty))

    # Only once the baseline exists — before that a climbing RSS is warm-up,
    # not a leak (§5.5), and penalising it would manufacture degradation.
    if rss_mb is not None and rss_baseline_mb:
        over = max(0.0, rss_mb - rss_baseline_mb)
        rss_penalty = min(RSS_CAP, over / RSS_DIVISOR)
        if rss_penalty >= 1.0:
            health -= rss_penalty
            reasons.append("rss +%.0fMB over baseline (-%.0f)" % (over, rss_penalty))

    health = max(0.0, min(100.0, health))

    if health < FAILED_BELOW:
        status = STATUS_FAILED
    elif health < DEGRADED_BELOW:
        status = STATUS_DEGRADED
    else:
        status = STATUS_HEALTHY
    return round(health, 1), status, reasons
