"""Unit tests for the simulated event generator (no Kafka, no DB)."""

from __future__ import annotations

import json

from streaming.params import StreamParams
from streaming.simulator import GPS_TOPIC, ORDERS_TOPIC, EventStream
from streaming.validation import VALID_EVENT_TYPES


def test_generation_is_deterministic():
    a = list(EventStream(seed=7).generate(300))
    b = list(EventStream(seed=7).generate(300))
    assert len(a) == 300
    assert [r.value for r in a] == [r.value for r in b]


def test_exactly_n_events_and_valid_shape():
    for raw in EventStream(seed=11).generate(400):
        assert raw.topic in (ORDERS_TOPIC, GPS_TOPIC)
        event = json.loads(raw.value)
        assert event["event_type"] in VALID_EVENT_TYPES
        assert event["city"] == "Bengaluru"
        assert event["simulated"] is True
        assert event["event_id"] and event["order_id"]
        if raw.topic == GPS_TOPIC:
            assert event["event_type"] == "gps_ping"


def test_lifecycle_has_created_and_delivered_per_order():
    raws = list(EventStream(seed=3).generate(2000))
    by_order: dict[str, list[str]] = {}
    for raw in raws:
        if raw.injected == "malformed":
            continue
        event = json.loads(raw.value)
        by_order.setdefault(event["order_id"], []).append(event["event_type"])
    assert by_order
    complete = [seq for seq in by_order.values() if "order_created" in seq and "delivered" in seq]
    assert complete, "expected at least one full lifecycle"
    for seq in complete:
        assert seq.index("order_created") == 0


def test_injections_fire_when_rates_high():
    params = StreamParams.load()
    params.data["inject_rates"] = {
        "duplicate": 0.3,
        "late": 0.3,
        "out_of_bounds": 0.3,
        "malformed": 0.3,
    }
    raws = list(EventStream(params=params, seed=5).generate(3000))
    injected = {r.injected for r in raws}
    assert {"duplicate", "late", "out_of_bounds", "malformed"} <= injected


def test_no_injections_when_rates_zero():
    params = StreamParams.load()
    params.data["inject_rates"] = {
        "duplicate": 0.0,
        "late": 0.0,
        "out_of_bounds": 0.0,
        "malformed": 0.0,
    }
    raws = list(EventStream(params=params, seed=5).generate(1500))
    assert all(r.injected is None for r in raws)
