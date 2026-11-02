"""Recovery agent — PHASE 6: diagnose, then decide via the simulator's rules.

Consumes cluster.health, diagnoses, routes through rule_based.assess_confidence
and emits an INTENT to cluster.actions. Tier 1 only — no LLM (Phase 7).

Still reads NO ground truth.

The single most important line in this file is the subscription list: it uses
AGENT_INPUT_TOPICS, which is [cluster.health] and deliberately excludes
cluster.failures. Ground truth is the answer key; an agent that reads it is
not diagnosing anything (plan.md §1).

Also serves a tiny HTTP endpoint so wait_for_warmup.sh can block on real
warm-up state instead of sleeping a guessed interval.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, "/app")

from common.kafka_io import (  # noqa: E402
    JsonProducer, ensure_topics, make_consumer, wait_for_kafka,
)
from common.schemas import (  # noqa: E402
    AGENT_INPUT_TOPICS, TOPIC_ACTION_RESULTS, TOPIC_ACTIONS, TOPIC_DIAGNOSES,
    TOPIC_FAILURES, TOPIC_SCORES,
)
from recovery_agent.classifier import Classifier  # noqa: E402
from recovery_agent.history import ClusterHistory  # noqa: E402
from recovery_agent.events import from_diagnosis  # noqa: E402
from recovery_agent.incidents import IncidentTracker  # noqa: E402
from recovery_agent.network_adapter import RealNetworkAdapter  # noqa: E402
from recovery_agent.outcomes import OutcomeTracker  # noqa: E402
from recovery_agent.policy import Policy, decision_id  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("agent")

GROUP_ID = os.getenv("AGENT_GROUP_ID", "recovery-agent")
DIAGNOSE_EVERY_S = float(os.getenv("DIAGNOSE_EVERY_S", "1.0"))
WARMUP_S = float(os.getenv("WARMUP_SECONDS", "60"))
STATUS_PORT = int(os.getenv("AGENT_STATUS_PORT", "9600"))

_shutdown = threading.Event()

HISTORY = ClusterHistory(warmup_s=WARMUP_S)
CLASSIFIER = Classifier(HISTORY)
INCIDENTS = IncidentTracker()
ADAPTER = RealNetworkAdapter(HISTORY)
PRODUCER = JsonProducer(client_id="recovery-agent")

# The simulator's OWN rule engine, imported unmodified from /simulator.
# Failing to import is fatal: silently falling back to a local reimplementation
# would quietly void the entire thesis.
from rule_based import RuleBasedRecovery  # noqa: E402
from scorer import DecisionScorer  # noqa: E402
from decision_memory import DecisionMemory  # noqa: E402
from predictor import HealthPredictor  # noqa: E402
from feedback import evaluate_action_result  # noqa: E402

POLICY = Policy(ADAPTER, HISTORY, RuleBasedRecovery())
def _publish_score(rec):
    PRODUCER.send(TOPIC_SCORES, rec, key=rec.get("target") or "")


OUTCOMES = OutcomeTracker(HISTORY, DecisionScorer(), DecisionMemory(),
                          evaluate_action_result, publish_score=_publish_score)
PREDICTOR = HealthPredictor()
PREDICT_EVERY_S = 3.0
_last_predict = [0.0]


def assert_no_ground_truth():
    """Fail at startup rather than quietly cheat.

    A subscription to cluster.failures would not crash anything — it would
    just make every number downstream meaningless while looking excellent.
    Cheap, load-bearing assertion.
    """
    if TOPIC_FAILURES in AGENT_INPUT_TOPICS:
        raise SystemExit(
            "FATAL: agent is configured to read %s. Ground truth is "
            "evaluator-only (plan.md §1)." % TOPIC_FAILURES)


def consume_results():
    """cluster.action_results — what the executor actually did. NOT ground
    truth: it reports our own actions' outcomes, never what the fault was."""
    consumer = make_consumer(GROUP_ID + "-results", [TOPIC_ACTION_RESULTS],
                             earliest=False)
    try:
        while not _shutdown.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                OUTCOMES.on_result(json.loads(msg.value().decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()


def consume_health():
    consumer = make_consumer(GROUP_ID, AGENT_INPUT_TOPICS, earliest=False)
    log.info("subscribed to %s (ground truth deliberately excluded)",
             AGENT_INPUT_TOPICS)
    try:
        while not _shutdown.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                HISTORY.ingest(json.loads(msg.value().decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()


def diagnose_loop():
    warned_warm = False
    while not _shutdown.is_set():
        _shutdown.wait(DIAGNOSE_EVERY_S)
        if _shutdown.is_set():
            break
        try:
            now = time.time()
            if not warned_warm and HISTORY.all_warm(now):
                log.info("all nodes warm — diagnosis active")
                warned_warm = True

            # Cluster-level shedding controller: not a per-node diagnosis,
            # so it runs beside the classifier rather than inside it.
            for intent in POLICY.shed_intents(now):
                PRODUCER.send(TOPIC_ACTIONS, intent, key=intent["target"])
                log.info("INTENT %s | %s on %s | source=%s | %s",
                         intent["decision_id"], intent["action"],
                         intent["target"], intent["source"], intent["reason"])

            # Score what is due, judge what is due, and re-decide anything
            # whose action did not work (per-action windows, §Phase 7).
            for intent, why in OUTCOMES.tick(ADAPTER, now):
                inc = INCIDENTS.get(intent["target"],
                                    (intent.get("diagnosis") or {}).get(
                                        "failure_type", ""))
                if inc is None:
                    continue
                follow = dict(intent)
                follow["attempt_no"] = inc.next_attempt()
                follow["decision_id"] = decision_id(inc.incident_id,
                                                    follow["attempt_no"])
                follow["decided_at"] = now
                follow["reason"] = "follow-up: %s" % why
                PRODUCER.send(TOPIC_ACTIONS, follow, key=follow["target"])
                log.info("FOLLOW-UP %s | attempt %d | %s on %s | %s",
                         follow["decision_id"], follow["attempt_no"],
                         follow["action"], follow["target"], why)

            # Predictive detection every 3s off the same rolling cache.
            if now - _last_predict[0] >= PREDICT_EVERY_S:
                _last_predict[0] = now
                try:
                    for alert in PREDICTOR.check_all_nodes(ADAPTER, now):
                        log.info("PREDICT %s | failure in ~%.1fs "
                                 "(confidence=%d%%, health=%.0f)",
                                 alert["node_id"], alert["ticks"],
                                 alert["confidence"], alert["health"])
                except Exception:
                    log.exception("predictor failed")

            diagnoses = CLASSIFIER.diagnose(now)
            latest = HISTORY.latest_all()
            new_incidents = INCIDENTS.observe(diagnoses, latest, now)

            # Only NEWLY OPENED incidents are announced. Re-diagnosis of an
            # open incident is suppressed (§5.4) — otherwise a memory leak
            # would emit a line every second for its entire lifetime.
            for inc in new_incidents:
                d = inc.last_diagnosis
                log.info(
                    "DIAGNOSIS %s | %s on %s | conf=%.2f%s | %s",
                    inc.incident_id, d.failure_type, d.target, d.confidence,
                    " AMBIGUOUS" if d.ambiguous else "", "; ".join(d.evidence),
                )
                rec = d.to_dict()
                rec["incident_id"] = inc.incident_id
                PRODUCER.send(TOPIC_DIAGNOSES, rec, key=d.target)

                intent = POLICY.intent_for(d, inc)
                if intent is None:
                    continue
                PRODUCER.send(TOPIC_ACTIONS, intent, key=intent["target"])
                OUTCOMES.register(intent, from_diagnosis(d, inc), ADAPTER)
                log.info("INTENT %s | %s on %s | source=%s conf=%d%% | "
                         "expected boot=%s status=%s",
                         intent["decision_id"], intent["action"],
                         intent["target"], intent["source"],
                         intent["confidence"],
                         (intent["expected_boot_id"] or "?")[:8],
                         intent["expected_target_status"])
        except Exception as exc:                      # never die on one bad cycle
            log.exception("diagnose cycle failed: %s", exc)


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        now = time.time()
        if self.path.startswith("/warmup"):
            report = HISTORY.warm_report(now)
            body = {
                "all_warm": HISTORY.all_warm(now),
                "remaining": {k: round(v, 1) for k, v in sorted(report.items())},
                "warmup_seconds": WARMUP_S,
            }
        elif self.path.startswith("/incidents"):
            body = INCIDENTS.summary()
        else:
            body = {
                "nodes": sorted(HISTORY.latest_all().keys()),
                "stale": HISTORY.stale(now),
                "leader": HISTORY.leader(),
                "all_warm": HISTORY.all_warm(now),
                "open_incidents": len(INCIDENTS.open_incidents()),
                "consensus": ADAPTER.consensus_snapshot(),
            }
        payload = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *a):
        pass          # the access log would drown the diagnosis log


def _handle_signal(signum, _frame):
    log.info("signal %d, shutting down", signum)
    _shutdown.set()


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    assert_no_ground_truth()

    if not wait_for_kafka():
        log.error("kafka unreachable; exiting")
        sys.exit(1)

    # Create topics before anything subscribes or produces (§4.2).
    ensure_topics()

    log.info("PHASE 7: diagnose -> tier 1 rules / tier 2-3 LLM -> actions "
             "-> score -> feedback -> memory. Still no ground truth.")
    log.info("warm-up = %.0fs per node (§5.5); nothing is emitted before it", WARMUP_S)

    threading.Thread(target=consume_health, name="health", daemon=True).start()
    threading.Thread(target=consume_results, name="results", daemon=True).start()
    threading.Thread(target=diagnose_loop, name="diagnose", daemon=True).start()

    server = HTTPServer(("0.0.0.0", STATUS_PORT), StatusHandler)
    threading.Thread(target=server.serve_forever, name="status", daemon=True).start()
    log.info("status endpoint on :%d (/warmup, /incidents)", STATUS_PORT)

    _shutdown.wait()
    server.shutdown()
    log.info("stopped. incidents=%s outcomes=%s",
             INCIDENTS.summary(), OUTCOMES.stats())


if __name__ == "__main__":
    main()
