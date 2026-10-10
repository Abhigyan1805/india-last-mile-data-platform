"""Kafka producer helpers: topic management and the simulated event feed."""

from __future__ import annotations

import time
from collections.abc import Iterable

from streaming.simulator import EventStream

DEFAULT_TOPICS = ("orders.events", "gps.pings")


def ensure_topics(
    bootstrap: str,
    topics: Iterable[str] = DEFAULT_TOPICS,
    partitions: int = 3,
    recreate: bool = False,
) -> None:
    """Create the contract topics, optionally deleting any existing ones first.

    ``recreate=True`` gives each recorded run a clean topic so offsets and
    counts are unambiguous (single-broker dev setup).
    """

    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15000)
    names = list(topics)
    existing = set(admin.list_topics())
    if recreate:
        to_delete = [t for t in names if t in existing]
        if to_delete:
            admin.delete_topics(to_delete)
            time.sleep(2)
        existing = set(admin.list_topics())
    to_create = [t for t in names if t not in existing]
    if to_create:
        admin.create_topics(
            [NewTopic(name=t, num_partitions=partitions, replication_factor=1) for t in to_create]
        )
    admin.close()


def produce(stream: EventStream, n_events: int, bootstrap: str) -> dict:
    """Send ``n_events`` simulated messages and return counters + timing."""

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=100,
        batch_size=131072,
        retries=3,
        max_in_flight_requests_per_connection=5,
    )
    errors: list[str] = []
    produced = 0
    injected: dict[str, int] = {}
    started = time.time()
    for raw in stream.generate(n_events):
        producer.send(
            raw.topic,
            key=raw.key.encode("utf-8"),
            value=raw.value,
        ).add_errback(lambda exc: errors.append(str(exc)))
        produced += 1
        injected[raw.injected or "clean"] = injected.get(raw.injected or "clean", 0) + 1
    producer.flush()
    producer.close()
    duration = time.time() - started
    return {
        "produced": produced,
        "injected": injected,
        "errors": errors,
        "duration_s": duration,
        "events_per_s": produced / duration if duration else 0.0,
    }
