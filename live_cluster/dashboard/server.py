"""Live dashboard. plan.md Phase 9.

Consumes all six topics and correlates them into the one view that IS the
thesis: for each incident, what actually happened (ground truth), what the
agent inferred without seeing it, what it decided, what the executor did, and
how it scored.

GROUND TRUTH IS SHOWN HERE ON PURPOSE, and labelled as such. The dashboard is
not the agent — it is the observer. Displaying the answer key beside the
agent's answer is the whole point of the exercise; it is only forbidden on the
agent's own input path (plan.md §1).

Stdlib only: no Flask, so the agent image's dependency set is unchanged.
"""

import json
import logging
import os
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, "/app")

from common.kafka_io import make_consumer, wait_for_kafka  # noqa: E402
from common.schemas import (  # noqa: E402
    TOPIC_ACTION_RESULTS, TOPIC_ACTIONS, TOPIC_DIAGNOSES, TOPIC_FAILURES,
    TOPIC_HEALTH, TOPIC_SCORES,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("dashboard")

PORT = int(os.getenv("DASHBOARD_PORT", "9700"))
HERE = os.path.dirname(os.path.abspath(__file__))
KEEP = 400

_shutdown = threading.Event()
_lock = threading.Lock()

NODES = {}                      # node_id -> latest health message
GT = {}                         # gt_id -> ground-truth record
DIAGNOSES = deque(maxlen=KEEP)
INTENTS = {}                    # decision_id -> intent
RESULTS = {}                    # decision_id -> [state records]
SCORES = {}                     # decision_id -> score record
EVENTS = deque(maxlen=KEEP)     # flat, ordered narrative


def add_event(kind, ts, target, text, detail="", source=""):
    with _lock:
        EVENTS.append({"kind": kind, "ts": ts, "target": target,
                       "text": text, "detail": detail, "source": source})


def on_message(topic, d):
    now = d.get("ts") or d.get("decided_at") or d.get("injected_at") or time.time()

    if topic == TOPIC_HEALTH:
        with _lock:
            NODES[d["node_id"]] = d
        return

    if topic == TOPIC_FAILURES:
        gid = d.get("gt_id")
        if not gid:
            return
        with _lock:
            GT.setdefault(gid, {}).update(d)
        if d.get("event") == "injected":
            add_event("truth", d.get("injected_at", now), d.get("target", ""),
                      "INJECTED %s" % d.get("failure_type"),
                      "ground truth — the agent never sees this")
        else:
            add_event("truth", d.get("resolved_at", now),
                      GT.get(gid, {}).get("target", ""),
                      "RESOLVED by %s" % d.get("resolved_by"), "")
        return

    if topic == TOPIC_DIAGNOSES:
        with _lock:
            DIAGNOSES.append(d)
        add_event("diagnosis", d.get("observed_at", now), d.get("target", ""),
                  "DIAGNOSED %s  %.0f%%" % (d.get("failure_type"),
                                            100 * (d.get("confidence") or 0)),
                  "; ".join(d.get("evidence") or []),
                  "ambiguous" if d.get("ambiguous") else "")
        return

    if topic == TOPIC_ACTIONS:
        with _lock:
            INTENTS[d["decision_id"]] = d
        rule = d.get("rule_suggestion")
        diverged = rule and rule != d.get("action")
        add_event("decision", d.get("decided_at", now), d.get("target", ""),
                  "DECIDED %s" % d.get("action"),
                  (d.get("reason") or "")[:140]
                  + ("   [DIVERGED from rule: %s]" % rule if diverged else ""),
                  d.get("source", ""))
        return

    if topic == TOPIC_ACTION_RESULTS:
        did = d.get("decision_id")
        with _lock:
            RESULTS.setdefault(did, []).append(d)
            target = (INTENTS.get(did) or {}).get("target", "")
        if d.get("state") in ("completed", "failed", "rejected", "reconciled"):
            add_event("action", now, target,
                      "EXECUTED %s" % d.get("state"),
                      d.get("detail") or d.get("rejected_reason") or "")
        return

    if topic == TOPIC_SCORES:
        did = d.get("decision_id")
        with _lock:
            SCORES[did] = d
        add_event("score", now, d.get("target", ""),
                  "SCORED %.0f" % (d.get("total_score") or 0),
                  "speed %.0f · stability %.0f · cascade %.0f · delivery %.0f"
                  % (d.get("speed_score") or 0, d.get("stability_score") or 0,
                     d.get("cascade_score") or 0, d.get("delivery_score") or 0))


def consume_all():
    topics = [TOPIC_HEALTH, TOPIC_FAILURES, TOPIC_DIAGNOSES, TOPIC_ACTIONS,
              TOPIC_ACTION_RESULTS, TOPIC_SCORES]
    c = make_consumer("dashboard-%d" % int(time.time()), topics, earliest=True)
    log.info("consuming %s", topics)
    try:
        while not _shutdown.is_set():
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            try:
                on_message(m.topic(), json.loads(m.value().decode("utf-8")))
            except Exception:
                continue
    finally:
        c.close()


def build_state():
    now = time.time()
    with _lock:
        nodes = []
        for nid, d in sorted(NODES.items()):
            age = now - (d.get("ts") or now)
            nodes.append({
                "id": nid, "role": d.get("role"),
                # A node that stopped publishing is failed regardless of the
                # last status it managed to send (§4.5).
                "status": "failed" if age > 5 else d.get("status"),
                "health": d.get("health"), "load": d.get("load"),
                "rss_mb": d.get("rss_mb"), "cpu_pct": d.get("cpu_pct"),
                "leader": bool(d.get("raft_leader")),
                "term": d.get("raft_term"), "quorum": d.get("has_quorum"),
                "apply_lag": d.get("apply_lag"), "shedding": d.get("shedding"),
                "isolated": d.get("isolated"), "stale_s": round(age, 1),
            })

        # Accuracy, computed the honest way: compare each diagnosis to the
        # ground truth whose window it falls in. Agreement is not assumed.
        hits = misses = 0
        for g in GT.values():
            if g.get("event") != "injected" and "injected_at" not in g:
                continue
            t0 = g.get("injected_at") or 0
            match = [x for x in DIAGNOSES
                     if x.get("target") == g.get("target")
                     and t0 <= (x.get("observed_at") or 0) <= t0 + 30]
            if any(x.get("failure_type") == g.get("failure_type") for x in match):
                hits += 1
            else:
                misses += 1

        events = list(EVENTS)[-120:]
        decisions = []
        for did, i in sorted(INTENTS.items(),
                             key=lambda kv: kv[1].get("decided_at", 0))[-25:]:
            states = [r.get("state") for r in RESULTS.get(did, [])]
            sc = SCORES.get(did) or {}
            decisions.append({
                "decision_id": did, "incident_id": i.get("incident_id"),
                "action": i.get("action"), "target": i.get("target"),
                "source": i.get("source"), "confidence": i.get("confidence"),
                "reason": i.get("reason"),
                "rule_suggestion": i.get("rule_suggestion"),
                "diverged": bool(i.get("rule_suggestion")
                                 and i["rule_suggestion"] != i.get("action")),
                "states": states, "score": sc.get("total_score"),
            })
        return {
            "now": now, "nodes": nodes, "events": list(reversed(events)),
            "decisions": list(reversed(decisions)),
            "stats": {
                "ground_truth": len([g for g in GT.values() if g.get("injected_at")]),
                "diagnoses": len(DIAGNOSES),
                "decisions": len(INTENTS),
                "scored": len(SCORES),
                "correct": hits, "missed": misses,
            },
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/state"):
            body = json.dumps(build_state()).encode()
            ctype = "application/json"
        else:
            with open(os.path.join(HERE, "index.html"), "rb") as fh:
                body = fh.read()
            ctype = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    if not wait_for_kafka():
        log.error("kafka unreachable")
        sys.exit(1)
    threading.Thread(target=consume_all, name="consume", daemon=True).start()
    log.info("dashboard on http://localhost:%d", PORT)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
