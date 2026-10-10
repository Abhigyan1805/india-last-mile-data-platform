"""Simulated Porter-style Bengaluru order lifecycle producer source.

Generates the two contract topics:

* ``orders.events`` - lifecycle events ``order_created`` -> ``driver_assigned``
  -> ``picked_up`` -> ``delivered`` (plus injected duplicates / late copies).
* ``gps.pings`` - ``gps_ping`` positions sampled every few seconds while the
  order is in transit.

Everything is *simulated* and every payload carries ``"simulated": true``.  The
demand curve and delivery-time distribution come from
:class:`streaming.params.StreamParams` (warehouse-derived or JSON fallback).

Fault injection is deliberate and configurable (``inject_rates``):

* ``duplicate`` - the exact same message is emitted twice (tests idempotent
  dedup keyed on ``event_id``);
* ``late`` - an event timestamped well before the running watermark;
* ``out_of_bounds`` - coordinates pushed outside the India bounding box;
* ``malformed`` - a payload missing a required field.

The generator is deterministic for a given seed, so tests can assert on exact
output and the recorded run is reproducible.
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from streaming.params import StreamParams
from streaming.validation import to_iso_z

ORDERS_TOPIC = "orders.events"
GPS_TOPIC = "gps.pings"

_LIFECYCLE = ("order_created", "driver_assigned", "picked_up", "delivered")
_MAX_PINGS_PER_ORDER = 300
_LATE_BACKDATE_S = 900.0
_TRAFFIC_CHOICES = (("Low", 0.30), ("Medium", 0.35), ("High", 0.25), ("Jam", 0.10))
_WEATHER_CHOICES = (
    ("Sunny", 0.45),
    ("Cloudy", 0.25),
    ("Windy", 0.10),
    ("Fog", 0.08),
    ("Sandstorms", 0.06),
    ("Stormy", 0.06),
)
# Fixed simulation epoch so runs are reproducible (UTC, a weekday in 2026).
_BASE_EPOCH = datetime(2026, 10, 1, 0, 0, 0, tzinfo=UTC).timestamp()


@dataclass
class RawEvent:
    """One Kafka message ready to send."""

    topic: str
    key: str
    value: bytes
    event_type: str
    injected: str | None = None


def _weighted(rng: random.Random, choices: tuple[tuple[str, float], ...]) -> str:
    labels, weights = zip(*choices, strict=True)
    return rng.choices(labels, weights=weights, k=1)[0]


class EventStream:
    """Deterministic generator of simulated order-lifecycle Kafka messages."""

    def __init__(
        self,
        params: StreamParams | None = None,
        seed: int = 42,
        base_epoch: float | None = None,
    ) -> None:
        self.params = params or StreamParams.load()
        self.rng = random.Random(seed)
        self.base_epoch = float(base_epoch if base_epoch is not None else _BASE_EPOCH)
        self._rates = self.params.inject_rates
        self._vehicles = tuple(self.params.vehicle_mix.items())
        self._order_seq = 0

    # -- public --------------------------------------------------------------
    def generate(self, n_events: int) -> Iterator[RawEvent]:
        """Yield exactly ``n_events`` messages (duplicates counted)."""

        emitted = 0
        while emitted < n_events:
            for raw in self._one_order():
                yield raw
                emitted += 1
                if emitted >= n_events:
                    return

    # -- internals -----------------------------------------------------------
    def _one_order(self) -> Iterator[RawEvent]:
        rng = self.rng
        params = self.params
        self._order_seq += 1
        order_id = f"BLR{self._order_seq:07d}"
        vehicle = rng.choices(
            [v for v, _ in self._vehicles], weights=[w for _, w in self._vehicles], k=1
        )[0]
        driver_id = f"DRV-{rng.randrange(params.data['driver_pool']):05d}"
        traffic = _weighted(rng, _TRAFFIC_CHOICES)
        weather = _weighted(rng, _WEATHER_CHOICES)

        hour = rng.choices(range(24), weights=params.hour_weights, k=1)[0]
        order_ts = self.base_epoch + hour * 3600 + rng.uniform(0, 3600)

        base_min = params.traffic_base_min[traffic]
        delivery_min = base_min * params.weather_multiplier[weather] * rng.gauss(1.0, 0.12)
        delivery_s = max(5.0, delivery_min) * 60.0

        center_lat, center_lon = params.center
        spread = params.spread_deg
        pickup = (
            center_lat + rng.uniform(-spread, spread),
            center_lon + rng.uniform(-spread, spread),
        )
        drop = (
            center_lat + rng.uniform(-spread, spread),
            center_lon + rng.uniform(-spread, spread),
        )

        seq = 0

        def make(
            event_type: str, ts: float, lat: float | None, lon: float | None, speed: float | None
        ) -> dict:
            nonlocal seq
            seq += 1
            return {
                "event_id": f"{order_id}:{event_type}:{seq}",
                "event_type": event_type,
                "order_id": order_id,
                "driver_id": driver_id,
                "vehicle_type": vehicle,
                "city": params.city,
                "event_ts": to_iso_z(ts),
                "lat": lat,
                "lon": lon,
                "speed_kmh": speed,
                "traffic": traffic,
                "weather": weather,
                "simulated": True,
            }

        assigned_ts = order_ts + rng.uniform(*params.data["dispatch_delay_s"])
        pickup_ts = assigned_ts + rng.uniform(*params.data["pickup_delay_s"])
        drop_ts = pickup_ts + delivery_s

        timeline: list[tuple[str, float, float, float, float | None]] = [
            ("order_created", order_ts, pickup[0], pickup[1], None),
            ("driver_assigned", assigned_ts, pickup[0], pickup[1], params.speed_kmh(vehicle)),
            ("picked_up", pickup_ts, pickup[0], pickup[1], params.speed_kmh(vehicle)),
        ]
        interval = params.ping_interval(vehicle)
        t = pickup_ts + interval
        pings = 0
        while t < drop_ts and pings < _MAX_PINGS_PER_ORDER:
            frac = (t - pickup_ts) / delivery_s if delivery_s else 0.0
            lat = pickup[0] + (drop[0] - pickup[0]) * frac
            lon = pickup[1] + (drop[1] - pickup[1]) * frac
            timeline.append(("gps_ping", t, lat, lon, params.speed_kmh(vehicle)))
            pings += 1
            t += interval
        timeline.append(("delivered", drop_ts, drop[0], drop[1], 0.0))

        for event_type, ts, lat, lon, speed in timeline:
            event = make(event_type, ts, lat, lon, speed)
            topic = GPS_TOPIC if event_type == "gps_ping" else ORDERS_TOPIC
            key = driver_id if event_type == "gps_ping" else order_id

            # -- out_of_bounds: push coordinates far outside India -------
            if lat is not None and rng.random() < self._rates["out_of_bounds"]:
                event["lat"] = lat + 55.0
                event["lon"] = lon + 60.0
                yield RawEvent(topic, key, _dumps(event), event_type, "out_of_bounds")
                continue

            # -- late: backdate the event well before the watermark ------
            injected = None
            if rng.random() < self._rates["late"]:
                event["event_ts"] = to_iso_z(ts - _LATE_BACKDATE_S)
                injected = "late"

            # -- malformed: drop a required field ------------------------
            if rng.random() < self._rates["malformed"]:
                payload = {k: v for k, v in event.items() if k != "event_id"}
                yield RawEvent(topic, key, _dumps(payload), event_type, "malformed")
                continue

            value = _dumps(event)
            yield RawEvent(topic, key, value, event_type, injected)

            # -- duplicate: emit the exact same bytes again --------------
            if rng.random() < self._rates["duplicate"]:
                yield RawEvent(topic, key, value, event_type, "duplicate")


def _dumps(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def new_event_id() -> str:
    """Fallback id for ad-hoc events (kept for parity with the producer)."""

    return uuid.uuid4().hex
