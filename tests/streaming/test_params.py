"""Unit tests for parameter loading and the JSON fallback."""

from __future__ import annotations

from streaming.params import StreamParams, _weights_from_counts


def test_load_defaults_from_json():
    params = StreamParams.load()
    assert len(params.hour_weights) == 24
    assert sum(params.hour_weights) > 0
    assert params.sources == {"demand": "json", "delivery_time": "json"}
    assert params.allowed_lateness_s > 0
    assert params.traffic_base_min and params.weather_multiplier
    assert set(params.vehicle_mix) == {"two_wheeler", "truck"}


def test_unreachable_db_falls_back_to_json():
    params = StreamParams.load(pg_dsn="host=127.0.0.1 port=1 dbname=nope connect_timeout=1")
    assert params.sources["demand"] == "json"


def test_weights_from_counts_pads_missing_hours():
    weights = _weights_from_counts([(0, 5.0), (23, 10.0)])
    assert len(weights) == 24
    assert sum(weights) > 0
    assert all(w > 0 for w in weights)


def test_weights_from_empty_is_empty():
    assert _weights_from_counts([]) == []
