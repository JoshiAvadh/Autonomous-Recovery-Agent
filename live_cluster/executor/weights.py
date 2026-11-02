"""Load-generator weight control. plan.md §6.2a, Phase 8.

`reroute_traffic` and `rebalance_load` act on THIS, not on the node. That is
why §6.2a validates them against the weight table rather than gating them on
node reachability — routing traffic away from a dead node is exactly the right
thing to do, so an unreachable target must not block them.
"""

import logging

import grpc

log = logging.getLogger("weights")


class WeightClient:
    def __init__(self, host, port, loadgen_pb2, loadgen_pb2_grpc):
        self._addr = "%s:%d" % (host, port)
        self._pb2 = loadgen_pb2
        self._stub_cls = loadgen_pb2_grpc.LoadGenServiceStub
        self._channel = None

    def _stub(self):
        if self._channel is None:
            self._channel = grpc.insecure_channel(self._addr)
        return self._stub_cls(self._channel)

    def get(self):
        r = self._stub().GetWeights(self._pb2.Empty(), timeout=5.0)
        return dict(r.weights)

    def set(self, weights):
        r = self._stub().SetWeights(
            self._pb2.WeightTable(weights=weights), timeout=5.0)
        return r.ok, r.detail

    # ---------------------------------------------------------- operations

    def reroute_away_from(self, target, fraction=0.5):
        """Move `fraction` of the target's share to the other nodes.

        A DELTA, not an assertion (§6.4) — it cannot be expressed as desired
        state, so it is the one action that stays imperative.
        """
        w = self.get()
        if target not in w:
            return False, "target %s absent from the weight table" % target
        moved = w[target] * fraction
        if moved <= 0:
            return False, "target already at zero share"
        others = [n for n in w if n != target]
        if not others:
            return False, "no other node to receive the traffic"
        w[target] -= moved
        per = moved / len(others)
        for n in others:
            w[n] += per
        ok, detail = self.set(w)
        return ok, "moved %.2f off %s to %d nodes (%s)" % (
            moved, target, len(others), detail[:80])

    def rebalance(self, exclude=()):
        """Flatten every non-excluded node to an equal share."""
        w = self.get()
        keep = [n for n in w if n not in exclude]
        if not keep:
            return False, "nothing to rebalance"
        share = 1.0 / len(keep)
        new = {n: (share if n in keep else 0.0) for n in w}
        ok, detail = self.set(new)
        return ok, "flattened %d nodes to %.3f each%s" % (
            len(keep), share,
            (", excluded %s" % list(exclude)) if exclude else "")

    def zero(self, target):
        """Stop sending to a node. Paired with ENTER_SHED so the node refuses
        AND the generator stops offering — belt and braces, because either
        alone leaves half the traffic path live."""
        w = self.get()
        if target not in w:
            return False, "target %s absent" % target
        w[target] = 0.0
        ok, detail = self.set(w)
        return ok, "zeroed %s (%s)" % (target, detail[:80])

    def restore(self, target):
        w = self.get()
        if target not in w:
            return False, "target %s absent" % target
        live = [n for n in w if w[n] > 0]
        share = (sum(w[n] for n in live) / len(live)) if live else 1.0
        w[target] = share
        ok, detail = self.set(w)
        return ok, "restored %s to %.3f" % (target, share)
