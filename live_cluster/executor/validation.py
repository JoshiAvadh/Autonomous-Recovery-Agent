"""Action-aware validation. plan.md §6.2 / §6.2a.

The governing rule: **`already_satisfied` may only be returned when the observed
change IS the action's effect.** A boot_id change proves a restart happened; it
proves nothing about whether traffic was rerouted or a node was shed. Applied
unconditionally it would silently drop every unrelated intent that happened to
straddle a restart.

`already_satisfied` and `stale` mean OPPOSITE things downstream — the goal is
met vs re-decide — so conflating them is the actual bug, not a style choice.

Some actions do not act on the node at all, so gating them on node reachability
is a category error: `reroute_traffic` and `rebalance_load` mutate the load
generator's weight table, and routing traffic away from a dead node is exactly
the right thing to do.
"""

ACCEPT = "accept"
DUPLICATE = "duplicate"
STALE = "stale"
ALREADY_SATISFIED = "already_satisfied"
SUPERSEDED = "superseded"
UNREACHABLE = "unreachable"

# What each action acts on. Weight-table actions skip the node gate entirely.
ACTS_ON_NODE = {
    "restart_node", "ENSURE_RUNNING", "isolate_node", "rejoin_node",
    "load_shed", "load_shed_readmit", "promote_replica",
}
ACTS_ON_WEIGHTS = {"reroute_traffic", "rebalance_load"}
ACTS_ON_NOTHING = {"monitor_only"}

# Actions whose entire effect IS "the process restarted", so a boot_id change
# proves them done. Everything else must revalidate.
BOOT_CHANGE_SATISFIES = {"restart_node", "ENSURE_RUNNING", "promote_replica"}

# Actions for which an unreachable/failed target is the PRECONDITION, not a
# blocker.
UNREACHABLE_IS_PRECONDITION = {"restart_node", "ENSURE_RUNNING"}

# Actions a failed target has already effectively achieved.
UNREACHABLE_SATISFIES = {"isolate_node", "promote_replica", "load_shed"}


class Verdict:
    def __init__(self, decision, reason="", detail=""):
        self.decision = decision
        self.reason = reason
        self.detail = detail

    @property
    def accepted(self):
        return self.decision == ACCEPT

    def __repr__(self):
        return "<%s%s>" % (self.decision, (" " + self.reason) if self.reason else "")


def validate(intent, log, live_status, weight_table_targets=None):
    """intent: dict from cluster.actions. live_status: fresh GetStatus() result
    as a dict, or None if the node could not be reached.

    The fresh read is the actual safety boundary — target_generation is
    agent/evaluator metadata and the executor cannot interpret it (§6.2).
    """
    did = intent.get("decision_id")
    action = intent.get("action")
    target = intent.get("target")

    # 1. duplicate — Kafka is at-least-once, so redelivery is certain
    if log.seen(did):
        return Verdict(DUPLICATE, "already in the transition log",
                       "state=%s" % log.state_of(did))

    # 3-before-2 for the cheap checks: supersede needs no RPC
    decided_at = float(intent.get("decided_at") or 0.0)
    for other_decided_at, other_id in log.applied_for_target(target):
        if other_decided_at > decided_at:
            return Verdict(SUPERSEDED,
                           "a later decision for %s already applied" % target,
                           "newer=%s" % other_id)

    if action in ACTS_ON_NOTHING:
        return Verdict(ACCEPT, "no-op action")

    # 2. weight-table actions: the node is the SUBJECT, not the thing touched.
    # Validate against the load generator instead.
    if action in ACTS_ON_WEIGHTS:
        if weight_table_targets is not None and target not in weight_table_targets:
            return Verdict(STALE, "target absent from the weight table")
        return Verdict(ACCEPT, "weight-table action; node state is not the "
                               "relevant state")

    # 2. node actions: action-aware interpretation of the fresh read
    if live_status is None:
        expected = intent.get("expected_target_status")
        if action in UNREACHABLE_IS_PRECONDITION and expected == "failed":
            return Verdict(ACCEPT, "unreachable confirms the precondition")
        if action in UNREACHABLE_SATISFIES:
            return Verdict(ALREADY_SATISFIED,
                           "target is already out of service")
        return Verdict(UNREACHABLE, "target did not answer GetStatus")

    boot_now = live_status.get("boot_id")
    boot_expected = intent.get("expected_boot_id")
    if boot_expected and boot_now and boot_now != boot_expected:
        started_now = float(live_status.get("started_at") or 0.0)
        started_expected = float(intent.get("expected_started_at") or 0.0)
        forward = started_now > started_expected
        if action in BOOT_CHANGE_SATISFIES:
            return Verdict(ALREADY_SATISFIED,
                           "process already restarted",
                           "boot %s->%s forward=%s"
                           % (boot_expected[:8], boot_now[:8], forward))
        # The change is NOT this action's effect. Worse, for isolate/shed a
        # restart UNDOES the effect: the network namespace and the in-process
        # shed flag both die with the container (§6.2a). So this is stale —
        # re-decide — never satisfied.
        return Verdict(STALE,
                       "boot changed but that is not this action's effect",
                       "boot %s->%s" % (boot_expected[:8], boot_now[:8]))

    status_now = live_status.get("status")
    status_expected = intent.get("expected_target_status")
    if status_expected and status_now and status_now != status_expected:
        return Verdict(STALE, "status changed since the decision",
                       "%s -> %s" % (status_expected, status_now))

    return Verdict(ACCEPT, "fresh read matches the decision's expectations")
