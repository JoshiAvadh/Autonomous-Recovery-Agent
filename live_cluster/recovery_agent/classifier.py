"""Diagnose failures from telemetry alone. plan.md §5.

NO GROUND TRUTH. The agent never reads cluster.failures — inferring the
failure type is the thing under test, and being handed the label would reduce
the project to "given a labeled failure, pick an action", which the simulator
already answers.

Three rules differ from the plan's first draft because Phase 4 MEASURED them
(§5.5a). Each carries its evidence inline.
"""

import statistics
import time

from recovery_agent.history import STALE_AFTER_S

CRASH = "crash"
PARTITION = "network_partition"
LATENCY = "latency_spike"
LEAK = "memory_leak"
CPU = "cpu_overload"
AMBIGUOUS = "ambiguous"

# Hysteresis (§5.2): asymmetric on purpose — slow to declare, fast to clear.
# The right bias when the downstream action can cost availability.
K_CONFIRM = 3

TERM_DIVERGENCE = 3          # §5.5a: measured 24 vs 1 on a majority cut
MATCH_LAG_MIN = 5            # §5.5a: measured 0 -> 69
LEAK_MIN_SAMPLES = 8         # §5.5
LEAK_MIN_R2 = 0.9            # §5.5
LEAK_MIN_SLOPE_MB_S = 0.5
CPU_THRESHOLD = 85.0
CPU_SUSTAINED = 3
LATENCY_MULTIPLE = 3.0
LATENCY_MIN_REQUESTS = 20    # §5.5: below this p99 is undefined and we ABSTAIN
LATENCY_MIN_SAMPLE_REQS = 3  # a single sample's p99 needs at least this many
LATENCY_MIN_ABSOLUTE_MS = 50.0   # a ratio alone explodes against a ~0ms baseline
# RTT thresholds (§5.2b). A ~1ms baseline means a real delay is a 20x+ signal,
# so these can be strict without being knife-edge like the write-path ones.
RTT_MULTIPLE = 5.0
RTT_MIN_ABSOLUTE_MS = 20.0
LATENCY_MIN_BASELINE_S = 60.0


class Diagnosis:
    def __init__(self, target, failure_type, confidence, evidence,
                 ambiguous=False, observed_at=None, target_generation=0):
        self.target = target
        self.failure_type = failure_type
        self.confidence = round(float(confidence), 2)
        self.evidence = list(evidence)
        self.ambiguous = ambiguous or failure_type == AMBIGUOUS
        self.observed_at = observed_at if observed_at is not None else time.time()
        self.target_generation = target_generation

    def to_dict(self):
        return {
            "target": self.target,
            "failure_type": self.failure_type,
            "confidence": self.confidence,
            "ambiguous": self.ambiguous,
            "evidence": self.evidence,
            "observed_at": self.observed_at,
            "target_generation": self.target_generation,
        }

    def __repr__(self):
        return "<%s %s conf=%.2f%s>" % (
            self.failure_type, self.target, self.confidence,
            " AMBIGUOUS" if self.ambiguous else "")


def _linfit(points):
    """(slope, r2) for [(x, y)]. r2 is 0.0 when y is constant."""
    n = len(points)
    if n < 3:
        return 0.0, 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, 0.0
    sxy = sum((x - mx) * (y - my) for x, y in points)
    slope = sxy / sxx
    ss_tot = sum((y - my) ** 2 for y in ys)
    if ss_tot == 0:
        return slope, 0.0
    ss_res = sum((y - (my + slope * (x - mx))) ** 2 for x, y in points)
    return slope, max(0.0, 1.0 - ss_res / ss_tot)


def _monotonic_rising(values, minimum_rise=0):
    return (len(values) >= 3
            and all(b >= a for a, b in zip(values, values[1:]))
            and values[-1] - values[0] > minimum_rise)


class Classifier:
    def __init__(self, history):
        self.h = history
        # Consecutive-confirmation counters, one per (node, rule).
        self._streak = {}

    def _confirm(self, key, condition):
        """K consecutive agreeing samples to declare; ONE to clear (§5.2)."""
        if condition:
            self._streak[key] = self._streak.get(key, 0) + 1
        else:
            self._streak[key] = 0
        return self._streak[key] >= K_CONFIRM

    # ------------------------------------------------------------ the rules

    def diagnose(self, now=None):
        now = now if now is not None else time.time()
        latest = self.h.latest_all()
        live = set(self.h.live(now))
        stale = self.h.stale(now)
        out = []

        for node_id in sorted(set(list(latest.keys()) + stale)):
            d = self._diagnose_node(node_id, latest, live, stale, now)
            if d:
                out.append(d)
        return out

    def _diagnose_node(self, node_id, latest, live, stale, now):
        hist = self.h.node(node_id)
        if hist is None:
            return None

        gen = hist.generation
        is_stale = node_id in stale

        # ---- rules 1 & 6: the node has gone quiet -------------------------
        if is_stale:
            unreachable, known = self._peer_verdict(node_id, latest, live)
            # A crash is the ONE case where peers_reachable is trustworthy:
            # process death closes sockets, so peers get real FIN/RST
            # disconnects immediately. That is quite different from a DROP
            # partition, where an established connection just hangs (§5.5a).
            if known and unreachable == known:
                if self._confirm((node_id, CRASH), True):
                    return Diagnosis(
                        node_id, CRASH, 0.90,
                        ["silent for %.1fs" % (hist.age(now) or 0),
                         "all %d live peers report it unreachable" % known],
                        observed_at=now, target_generation=gen)
                return None
            last = hist.latest() or {}
            expects_corroboration = last.get("role") in ("Primary", "Replica")

            if known == 0 and not expects_corroboration:
                # The SERVING TIER has no peer who CAN name it. §4.6a: readonly
                # nodes appear to voters only as `readonly_node_status_server_0`
                # — an anonymous counter — so their rows are filtered out and
                # corroboration is structurally impossible, not merely absent.
                #
                # Silence from a node that publishes its own heartbeat still
                # means it is almost certainly dead. Call it a crash, but at
                # REDUCED confidence, and say why corroboration is missing
                # rather than claiming a certainty we do not have.
                #
                # This branch is gated on role deliberately. For a VOTER, peers
                # normally can and do name it, so `known == 0` means their
                # silence is itself unexplained — inferring a crash from an
                # absent row there would be exactly the "missing means
                # unreachable" mistake §4.1 warns about.
                if self._confirm((node_id, CRASH), True):
                    return Diagnosis(
                        node_id, CRASH, 0.70,
                        ["silent for %.1fs" % (hist.age(now) or 0),
                         "no peer can attest: serving tier is anonymous to "
                         "Raft peers (§4.6a), so no corroboration exists"],
                        observed_at=now, target_generation=gen)
                return None
            if known and unreachable == 0:
                # Rule 6: quiet, but peers can still reach it. That is a
                # publisher/telemetry fault, not a node failure — and calling
                # it a crash would trigger a restart nobody needed.
                if self._confirm((node_id, "quiet"), True):
                    return Diagnosis(
                        node_id, AMBIGUOUS, 0.40,
                        ["silent for %.1fs" % (hist.age(now) or 0),
                         "but %d peers still reach it — telemetry fault?" % known],
                        ambiguous=True, observed_at=now, target_generation=gen)
            return None

        self._streak.pop((node_id, CRASH), None)
        self._streak.pop((node_id, "quiet"), None)

        if not hist.warm(now):
            return None          # §5.5 warm-up: emit nothing at all

        msg = latest.get(node_id) or {}
        evidence = []

        # ---- rule 2: partition -------------------------------------------
        part = self._partition_signals(node_id, msg, latest)
        if self._confirm((node_id, PARTITION), part is not None):
            return Diagnosis(node_id, PARTITION, 0.85, part,
                             observed_at=now, target_generation=gen)

        # ---- rule 3: memory leak -----------------------------------------
        leak = self._leak_signals(hist, now)
        if self._confirm((node_id, LEAK), leak is not None):
            slope, r2 = leak[1], leak[2]
            conf = max(0.5, min(0.95, 0.5 + min(0.45, slope / 20.0) * r2))
            return Diagnosis(node_id, LEAK, conf, leak[0],
                             observed_at=now, target_generation=gen)

        # ---- rule 4: cpu overload ----------------------------------------
        cpu = self._cpu_signals(hist, now)
        if self._confirm((node_id, CPU), cpu is not None):
            return Diagnosis(node_id, CPU, 0.80, cpu,
                             observed_at=now, target_generation=gen)

        # ---- rule 5: latency spike ---------------------------------------
        lat = self._latency_signals(hist, now)
        if self._confirm((node_id, LATENCY), lat is not None):
            return Diagnosis(node_id, LATENCY, 0.75, lat,
                             observed_at=now, target_generation=gen)

        # ---- rule 7: something is wrong, nothing dominates ---------------
        if (msg.get("health", 100) < 70 or msg.get("status") == "degraded"):
            evidence.append("health=%.0f status=%s" % (msg.get("health", 0), msg.get("status")))
            if self._confirm((node_id, AMBIGUOUS), True):
                return Diagnosis(node_id, AMBIGUOUS, 0.45, evidence,
                                 ambiguous=True, observed_at=now, target_generation=gen)
        else:
            self._streak[(node_id, AMBIGUOUS)] = 0
        return None

    # -------------------------------------------------------------- helpers

    def _peer_verdict(self, node_id, latest, live):
        """How many LIVE peers say this node is unreachable, out of how many
        actually expressed an opinion.

        A missing row is UNKNOWN, never "unreachable" (§4.1): only nodes alive
        enough to publish assert anything, and a crashed node contributes no
        row of its own — treating absence as a vote would make one crash look
        like a total partition.
        """
        unreachable = known = 0
        for peer in live:
            if peer == node_id:
                continue
            pr = (latest.get(peer) or {}).get("peers_reachable") or {}
            if node_id not in pr:
                continue
            known += 1
            if pr[node_id] is False:
                unreachable += 1
        return unreachable, known

    def _partition_signals(self, node_id, msg, latest):
        """§5.5a. Keys on Raft semantics, NOT peers_reachable.

        Measured: a DROP partition leaves an established TCP connection intact,
        so the socket flag stayed True for 45s under a single-link cut. It does
        flip under a majority cut (the campaigning node churns connections) —
        i.e. it reports in the obvious case and stays silent in the subtle one,
        which is exactly backwards for a detector.

        What moves reliably: the isolated node campaigns forever so its term
        runs away (measured 24 vs 1), and the leader's match_lag for it grows
        without bound (measured 0 -> 69).
        """
        voters = [d for d in latest.values() if d.get("role") in ("Primary", "Replica")]
        if len(voters) < 2 or msg.get("role") not in ("Primary", "Replica"):
            return None

        terms = sorted(d.get("raft_term", 0) for d in voters)
        median_term = statistics.median(terms)
        my_term = msg.get("raft_term", 0)
        if my_term < median_term + TERM_DIVERGENCE:
            return None

        ev = ["raft_term %s vs cluster median %s" % (my_term, median_term)]

        # Corroborate with the leader's view. match_lag only grows while the
        # log is advancing, so this needs live write traffic — which is why
        # the load generator is not optional (Phase 3).
        leader = self.h.leader()
        ml = ((latest.get(leader) or {}).get("match_lag") or {}) if leader else {}
        lag = ml.get(node_id)
        if lag is not None and lag > MATCH_LAG_MIN:
            ev.append("leader match_lag=%s and growing" % lag)
        if msg.get("has_quorum") is False:
            ev.append("has_quorum=false (minority side)")

        # Term divergence alone can be an ordinary contested election, so
        # require at least one corroborating symptom.
        return ev if len(ev) >= 2 else None

    def _leak_signals(self, hist, now):
        samples = hist.window(60.0, now)
        pts = [(d["ts"], d["rss_mb"]) for d in samples
               if d.get("rss_mb") is not None and d.get("rss_baseline_mb") is not None]
        if len(pts) < LEAK_MIN_SAMPLES:
            # Baseline still None -> still warming -> ABSTAIN. Never compare
            # against a pre-cache-fill reading (§5.5).
            return None

        baseline = samples[-1].get("rss_baseline_mb")
        current = pts[-1][1]
        slope, r2 = _linfit(pts)
        if slope < LEAK_MIN_SLOPE_MB_S or r2 < LEAK_MIN_R2:
            return None
        if current <= baseline * 1.2:
            return None
        return (
            ["rss %.0f MB vs baseline %.0f MB" % (current, baseline),
             "rising %.2f MB/s over %ds (r2=%.2f)" % (slope, len(pts), r2)],
            slope, r2,
        )

    def _cpu_signals(self, hist, now):
        samples = hist.window(6.0, now)
        cpus = [d["cpu_pct"] for d in samples if d.get("cpu_pct") is not None]
        if len(cpus) < CPU_SUSTAINED:
            return None
        recent = cpus[-CPU_SUSTAINED:]
        if not all(c > CPU_THRESHOLD for c in recent):
            return None
        # rss flat — otherwise a leak's allocation churn can look like CPU load
        rss = [d["rss_mb"] for d in samples if d.get("rss_mb") is not None]
        if rss and _monotonic_rising(rss, minimum_rise=20):
            return None
        return ["cpu_pct %s sustained above %.0f%%" % (
            [round(c) for c in recent], CPU_THRESHOLD)]

    def _latency_signals(self, hist, now):
        """§5.2b — keys on PEER RTT, not req_p99_ms.

        Measured: req_p99_ms cannot carry this fault. A 150 ms path delay moved
        the leader's write p99 from 154 ms to 349 ms — 2.27x against a 3.0x
        threshold — because the injected delay is one leg of a round trip
        already dominated by the quorum baseline. Pushing the delay to clear 3x
        destabilises leadership instead, invalidating the baseline. The usable
        band was empty.

        RTT measures the path directly: ~1 ms baseline, ~151 ms under the same
        injection. It also ATTRIBUTES — a node whose RTT is elevated to most of
        its peers is itself delayed, which the write path could never say.
        """
        samples = hist.window(6.0, now)
        if len(samples) < K_CONFIRM:
            return None
        latest = samples[-1]
        rtt = latest.get("peer_rtt_ms") or {}
        base = latest.get("peer_rtt_baseline_ms") or {}

        elevated, measured = [], 0
        for peer, cur in rtt.items():
            b = base.get(peer)
            if cur is None or b is None:
                continue          # still building a baseline -> abstain
            measured += 1
            if cur >= RTT_MIN_ABSOLUTE_MS and cur >= b * RTT_MULTIPLE:
                elevated.append((peer, cur, b))

        if measured < 2 or not elevated:
            return None

        # MEASURED GUARD. The probe runs inside this node's own process, so
        # anything that starves its event loop inflates its own outbound RTT.
        # A CPU burn did exactly that: node-5 reported "rtt to 4/4 peers
        # elevated" three seconds BEFORE cpu_overload confirmed, producing a
        # false positive on a node whose real fault was saturation, not the
        # network. In-process probing cannot distinguish "the path is slow"
        # from "I am too busy to measure the path", so when CPU is saturated
        # the honest answer is to defer to cpu_overload.
        cpus = [d["cpu_pct"] for d in samples if d.get("cpu_pct") is not None]
        if cpus and max(cpus[-CPU_SUSTAINED:]) > CPU_THRESHOLD:
            return None

        # EVERY measured peer must be elevated, not merely most.
        #
        # The logic is exact rather than statistical: if this node's own path
        # is delayed, every peer is slow from here. A single peer that is still
        # fast PROVES the local path is fine, which makes the slow ones their
        # own problem — an observation, not this node's fault.
        #
        # "Strict majority" was not enough. Measured: with the real target
        # (node-3, 3/3 elevated) and a separately CPU-starved node-5 both slow,
        # node-2 saw 2-of-3 — a majority — and was wrongly diagnosed. It was
        # watching two independently slow peers, and node-4 was crashed so it
        # contributed no sample to dilute them. Requiring all peers attributes
        # to node-3 alone, which is what the matrix is for.
        if len(elevated) != measured:
            return None

        worst = max(elevated, key=lambda e: e[1])
        return ["rtt to %d/%d peers elevated" % (len(elevated), measured),
                "worst %s: %.0fms vs baseline %.1fms (%.0fx)"
                % (worst[0], worst[1], worst[2], worst[1] / max(worst[2], 0.01))]

    def _latency_signals_write_path_DEPRECATED(self, hist, now):
        """§5.2 rule 5, baselined PER NODE.

        The leader's p99 sits ~1000x above a follower's simply by being the
        leader (measured ~150ms vs 0.1ms — the quorum round-trip). A
        cluster-wide baseline would flag the leader permanently and make every
        failover look like a spike appearing and vanishing.
        """
        recent = hist.window(5.0, now)
        if not recent:
            return None

        # MEASURED FIX. Leadership determines the latency regime: the leader
        # serves writes and pays the quorum round-trip (~150ms), a follower
        # serves local reads (~0.1ms). So a node's own history is only a valid
        # baseline while its ROLE has not changed.
        #
        # Phase 5 gate run 2 injected latency on the leader, which cost it
        # leadership; node-2 was promoted, began serving writes, and its p99
        # went 0.03ms -> 198ms. Compared against its own follower history that
        # read as a 6585x spike — a false positive on the wrong node, caused by
        # a regime change rather than by any latency fault. Same class of bug
        # as the boot_id re-warm: a regime change invalidates prior history.
        now_leader = bool(recent[-1].get("raft_leader"))
        older = [d for d in hist.window(LATENCY_MIN_BASELINE_S, now)
                 if d["ts"] < now - 10.0
                 and bool(d.get("raft_leader")) == now_leader]
        if len(older) < 20:
            # Includes the just-promoted case: a new leader has no same-regime
            # history yet, so it ABSTAINS until it has built one.
            return None

        # Two floors, and both matter. Each published p99 is computed from ONE
        # 1s sample's own requests, so a sample with 2 requests carries a
        # meaningless percentile no matter how busy its neighbours were.
        # Summing across the window alone would let a couple of stale
        # high-volume samples license a spike call on thin current evidence.
        supported = [d for d in recent
                     if (d.get("req_ok") or 0) >= LATENCY_MIN_SAMPLE_REQS]
        cur_reqs = sum((d.get("req_ok") or 0) for d in supported)
        if cur_reqs < LATENCY_MIN_REQUESTS:
            # ABSTAIN rather than fire (§5.5) — this is why reqwindow returns
            # None, not 0.0: 0.0 would read as "fast".
            return None

        cur = [d["req_p99_ms"] for d in supported if d.get("req_p99_ms") is not None]
        base = [d["req_p99_ms"] for d in older if d.get("req_p99_ms") is not None]
        if not cur or len(base) < 10:
            return None

        cur_p99 = max(cur)
        base_p99 = statistics.median(base)
        # A ratio against a near-zero baseline explodes meaninglessly: 0.03ms
        # -> 0.09ms is a 3x "spike" nobody cares about. Require an ABSOLUTE
        # floor too, so the rule only fires on latency a client would notice.
        if cur_p99 < LATENCY_MIN_ABSOLUTE_MS:
            return None
        if base_p99 <= 0 or cur_p99 < base_p99 * LATENCY_MULTIPLE:
            return None

        # cpu and rss flat, else this is overload/leak showing up as latency
        cpus = [d["cpu_pct"] for d in recent if d.get("cpu_pct") is not None]
        if cpus and max(cpus) > CPU_THRESHOLD:
            return None
        return ["p99 %.0fms vs own baseline %.0fms (%.1fx)" % (
            cur_p99, base_p99, cur_p99 / base_p99)]
