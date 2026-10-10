"""Unit tests for the Kafka-free validation / dedup / watermark logic."""

from __future__ import annotations

import json

from streaming.validation import StreamProcessor, in_india_bbox, parse_event_ts, to_iso_z


def event(**kwargs) -> dict:
    base = {
        "event_id": "e1",
        "event_type": "gps_ping",
        "order_id": "o1",
        "driver_id": "d1",
        "vehicle_type": "two_wheeler",
        "city": "Bengaluru",
        "event_ts": "2026-10-01T08:00:00.000Z",
        "lat": 12.9,
        "lon": 77.6,
        "speed_kmh": 20.0,
    }
    base.update(kwargs)
    return base


def raw(obj) -> bytes:
    if isinstance(obj, (bytes, str)):
        return obj if isinstance(obj, bytes) else obj.encode()
    return json.dumps(obj).encode()


def test_invalid_json_is_malformed():
    p = StreamProcessor()
    d = p.process(b"{not json")
    assert d.reason == "malformed" and not d.landed
    assert p.malformed == 1 and p.landed == 0


def test_missing_required_field_is_malformed():
    e = event()
    del e["event_id"]
    p = StreamProcessor()
    assert p.process(raw(e)).reason == "malformed"


def test_bad_event_type_is_malformed():
    p = StreamProcessor()
    assert p.process(raw(event(event_type="teleported"))).reason == "malformed"


def test_bad_timestamp_is_malformed():
    p = StreamProcessor()
    assert p.process(raw(event(event_ts="yesterday"))).reason == "malformed"


def test_non_numeric_lat_is_malformed():
    p = StreamProcessor()
    assert p.process(raw(event(lat="north"))).reason == "malformed"


def test_valid_event_lands():
    p = StreamProcessor()
    d = p.process(raw(event()))
    assert d.landed and d.reason is None and not d.is_late
    assert p.landed == 1 and p.gps_pings == 1 and p.geolocated == 1


def test_out_of_bounds_is_rejected():
    p = StreamProcessor()
    d = p.process(raw(event(lat=0.0, lon=0.0)))
    assert d.reason == "out_of_bounds" and p.out_of_bounds == 1 and p.landed == 0


def test_duplicate_event_id_is_rejected():
    p = StreamProcessor()
    assert p.process(raw(event())).landed
    d = p.process(raw(event()))
    assert d.reason == "duplicate" and p.duplicates == 1 and p.landed == 1


def test_late_event_lands_flagged():
    p = StreamProcessor(allowed_lateness_s=300)
    p.process(raw(event(event_id="a", event_ts="2026-10-01T08:00:00Z")))
    d = p.process(raw(event(event_id="b", event_ts="2026-10-01T07:50:00Z")))
    assert d.landed and d.is_late and p.late == 1


def test_event_within_lateness_is_not_late():
    p = StreamProcessor(allowed_lateness_s=300)
    p.process(raw(event(event_id="a", event_ts="2026-10-01T08:00:00Z")))
    d = p.process(raw(event(event_id="b", event_ts="2026-10-01T07:58:00Z")))
    assert d.landed and not d.is_late and p.late == 0


def test_india_bbox():
    assert in_india_bbox(12.97, 77.59)
    assert not in_india_bbox(67.5, 77.59)
    assert not in_india_bbox(12.97, 200.0)


def test_parse_event_ts_matches_utc():
    assert parse_event_ts("1970-01-01T00:00:00Z") == 0.0
    epoch = parse_event_ts("2026-10-01T00:00:00Z")
    assert epoch > 0 and isinstance(epoch, float)
    assert to_iso_z(epoch) == "2026-10-01T00:00:00.000Z"
