"""Kafka consumer that validates, dedups and lands simulated events.

Thin wrapper: it polls Kafka and delegates every decision to
:class:`streaming.validation.StreamProcessor`, then writes landings/rejects via
:class:`streaming.db.StreamSink`.  All the interesting logic lives in those two
modules so it can be tested without a broker.
"""

from __future__ import annotations

import time

from streaming.db import StreamSink
from streaming.producer import DEFAULT_TOPICS
from streaming.validation import StreamProcessor


def consume(
    bootstrap: str,
    processor: StreamProcessor,
    sink: StreamSink,
    load_id: str,
    topics: tuple[str, ...] = DEFAULT_TOPICS,
    expected: int | None = None,
    idle_timeout_s: float = 20.0,
    max_poll_records: int = 500,
) -> dict:
    """Consume until ``expected`` messages are seen (or the stream goes idle)."""

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=f"ilm-stream-{load_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=max_poll_records,
    )
    consumer.subscribe(list(topics))

    consumed = 0
    started = time.time()
    last_message_at = started
    try:
        while True:
            batches = consumer.poll(timeout_ms=1000, max_records=max_poll_records)
            if batches:
                last_message_at = time.time()
                for records in batches.values():
                    for record in records:
                        now = time.time()
                        decision = processor.process(record.value)
                        if decision.landed:
                            sink.land(decision.event, decision.is_late, now)
                        else:
                            sink.reject(decision.event, decision.reason, now)
                        consumed += 1
                        if expected is not None and consumed >= expected:
                            raise _Done
            elif time.time() - last_message_at > idle_timeout_s:
                break
    except _Done:
        pass
    finally:
        consumer.close()
        sink.close()

    duration = time.time() - started
    stats = {
        "consumed": processor.consumed,
        "landed": processor.landed,
        "duplicates": processor.duplicates,
        "out_of_bounds": processor.out_of_bounds,
        "malformed": processor.malformed,
        "late": processor.late,
        "gps_pings": processor.gps_pings,
        "geolocated": processor.geolocated,
        "duration_s": duration,
        "events_per_s": consumed / duration if duration else 0.0,
    }
    return stats


class _Done(Exception):
    """Internal signal used to stop polling cleanly."""
