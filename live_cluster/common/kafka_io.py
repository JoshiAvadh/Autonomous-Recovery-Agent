"""Thin Kafka helpers shared by every service. plan.md §4.2.

Deliberately small: a JSON producer with a keyed send, and a consumer factory.
Everything else (ordering, dedup, idempotency) is handled at the layer that
owns the semantics, not here.
"""

import json
import logging
import os
import socket
import time
from typing import Callable, Iterable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def _delivery_report(err, msg):
    if err is not None:
        log.warning("kafka delivery failed: %s", err)


class JsonProducer:
    """Fire-and-forget JSON producer, keyed so a topic partitions by node."""

    def __init__(self, bootstrap: Optional[str] = None, client_id: Optional[str] = None):
        self._p = Producer(
            {
                "bootstrap.servers": bootstrap or BOOTSTRAP,
                "client.id": client_id or socket.gethostname(),
                # Small linger keeps 1 Hz telemetry from batching into jitter.
                "linger.ms": 5,
                "acks": "1",
            }
        )

    def send(self, topic: str, value: dict, key: Optional[str] = None):
        self._p.produce(
            topic,
            key=(key.encode("utf-8") if key else None),
            value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
            callback=_delivery_report,
        )
        # Serve delivery callbacks without blocking the caller.
        self._p.poll(0)

    def flush(self, timeout: float = 5.0):
        return self._p.flush(timeout)


def make_consumer(
    group_id: str,
    topics: Iterable[str],
    bootstrap: Optional[str] = None,
    earliest: bool = True,
) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap or BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if earliest else "latest",
            # The executor commits only AFTER its transition log is durable,
            # so it must own commit timing (plan.md §6.3).
            "enable.auto.commit": False,
        }
    )
    c.subscribe(list(topics))
    return c


def consume_loop(consumer: Consumer, handler: Callable[[str, dict], None], poll: float = 1.0):
    """Drive a consumer, decoding JSON and dispatching (topic, payload).

    Kafka is at-least-once: redelivery is CERTAIN, not hypothetical. Handlers
    must be idempotent on their own terms (plan.md §6.1).
    """
    while True:
        msg = consumer.poll(poll)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            log.warning("undecodable message on %s, skipping", msg.topic())
            continue
        handler(msg.topic(), payload)


def ensure_topics(bootstrap: Optional[str] = None, topics=None, timeout: float = 30.0):
    """Create every topic up front. plan.md §4.2 (1 partition, replication 1).

    Not cosmetic. Relying on auto-creation means a consumer can SUBSCRIBE to a
    topic that does not exist yet; librdkafka then refreshes metadata only
    every 5 minutes by default, and when the partition is finally assigned
    `auto.offset.reset` resolves against the current high watermark — which is
    already PAST any message produced in the meantime.

    That is exactly how the Phase 6 gate lost its only intent: the executor was
    assigned cluster.actions/0 with no committed offset, resolved "latest" to
    offset 1, and never read the message sitting at offset 0.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    from common.schemas import ALL_TOPICS

    topics = list(topics or ALL_TOPICS)
    admin = AdminClient({"bootstrap.servers": bootstrap or BOOTSTRAP})
    existing = set()
    try:
        existing = set(admin.list_topics(timeout=10.0).topics.keys())
    except Exception:
        pass

    todo = [NewTopic(t, num_partitions=1, replication_factor=1)
            for t in topics if t not in existing]
    if not todo:
        return True

    for topic, fut in admin.create_topics(todo).items():
        try:
            fut.result(timeout=timeout)
            log.info("created topic %s", topic)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                log.warning("could not create %s: %s", topic, exc)
    return True


def wait_for_kafka(bootstrap: Optional[str] = None, timeout: float = 120.0) -> bool:
    """Block until the broker answers a metadata request.

    Compose healthchecks already gate startup, but a node that races the broker
    would otherwise die in a restart loop and look like a crash to the very
    classifier we are trying to measure.
    """
    from confluent_kafka.admin import AdminClient

    deadline = time.time() + timeout
    admin = AdminClient({"bootstrap.servers": bootstrap or BOOTSTRAP})
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5.0)
            return True
        except Exception:
            time.sleep(2.0)
    return False
