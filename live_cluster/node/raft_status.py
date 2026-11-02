"""Project pysyncobj's getStatus() into the §4.6 telemetry fields.

pysyncobj already computes quorum, leadership, commit/applied indices, term,
and per-peer connectivity. None of that is reimplemented here — this module
only reshapes it.

The one subtlety is §4.6a: readonly nodes have no self-address, so pysyncobj
identifies them by an auto-incrementing counter. `readonly_node_status_server_0`
and `match_idx_server_0` cannot be attributed to a node, so every parser here
filters to ADDRESS-shaped keys and drops counter-shaped ones. The serving tier
reports its own apply_lag and reachability instead.
"""

from typing import Dict, Optional

# pysyncobj emits `<prefix><node identity>`. For voters the identity is an
# address ("node-2:9000"); for readonly nodes it is a bare counter ("0").
_PARTNER_PREFIX = "partner_node_status_server_"
_MATCH_PREFIX = "match_idx_server_"

# 2 = connected, 0 = not (syncobj.py:754)
_CONNECTED = 2


def _is_address_key(identity: str) -> bool:
    """True for 'node-2:9000', False for '0'.

    A bare counter is an anonymous readonly node (§4.6a) — it names nothing we
    can attribute telemetry to, so it is dropped rather than guessed at.
    """
    return ":" in identity


def _strip(key: str, prefix: str) -> str:
    return key[len(prefix):]


def peers_reachable(status: dict) -> Dict[str, bool]:
    """Address-keyed reachability, as seen BY THIS NODE.

    Voters report the other voters; readonly nodes report all voters. A node
    absent from this map is *unknown*, never "unreachable" — only nodes alive
    enough to publish assert anything (plan.md §4.1).
    """
    out = {}
    for key, val in status.items():
        if not key.startswith(_PARTNER_PREFIX):
            continue
        identity = _strip(key, _PARTNER_PREFIX)
        if not _is_address_key(identity):
            continue
        out[_host_of(identity)] = (val == _CONNECTED)
    return out


def match_lag(status: dict, is_leader: bool) -> Optional[Dict[str, int]]:
    """Leader-observed replication progress, VOTERS ONLY (§4.6a).

    Returns None on a follower: match indices are only meaningful on the
    leader, and a follower's stale copy would be actively misleading.
    """
    if not is_leader:
        return None

    leader_commit = status.get("leader_commit_idx")
    if leader_commit is None:
        leader_commit = status.get("commit_idx", 0)

    out = {}
    for key, val in status.items():
        if not key.startswith(_MATCH_PREFIX):
            continue
        identity = _strip(key, _MATCH_PREFIX)
        if not _is_address_key(identity):
            continue  # anonymous readonly node — §4.6a
        out[_host_of(identity)] = max(0, leader_commit - int(val))
    return out


def apply_lag(status: dict) -> int:
    """Self-observed read freshness: leader_commit_idx - last_applied.

    Works identically on voters and readonly nodes — the probe in §4.6a
    confirmed the serving tier reports both fields.
    """
    leader_commit = status.get("leader_commit_idx")
    if leader_commit is None:
        leader_commit = status.get("commit_idx", 0)
    last_applied = status.get("last_applied", 0)
    return max(0, int(leader_commit) - int(last_applied))


def _host_of(address: str) -> str:
    """'node-2:9000' -> 'node-2'. Telemetry is keyed by node id, not endpoint."""
    return address.rsplit(":", 1)[0]


def leader_id(status: dict) -> Optional[str]:
    """Node id of the current leader, or None if there is no leader."""
    leader = status.get("leader")
    if not leader:
        return None
    return _host_of(str(leader))


def project(status: dict, is_leader: bool) -> dict:
    """All §4.6 fields in one pass."""
    return {
        "raft_leader": is_leader,
        "raft_term": int(status.get("raft_term", 0)),
        "commit_index": int(status.get("commit_idx", 0)),
        "leader_commit_idx": status.get("leader_commit_idx"),
        "has_quorum": bool(status.get("has_quorum", False)),
        "leader_id": leader_id(status),
        "apply_lag": apply_lag(status),
        "match_lag": match_lag(status, is_leader),
        "peers_reachable": peers_reachable(status),
        # Note: uptime here is SyncObj's, not the process's. The process's own
        # uptime (and boot_id) come from node_service — plan.md §4.1a is
        # explicit that restart proofs never key on a duration.
    }
