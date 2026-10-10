"""Simulated Porter-style Bengaluru order lifecycle producer source.

Generates the two contract topics:

* ``orders.events`` - lifecycle events ``order_created`` -> ``driver_assigned``
  -> ``picked_up`` -> ``delivered`` (plus injected duplicates / late copies).
* ``gps.pings`` - ``gps_ping`` positions sampled every few seconds while the
  order is in transit.

Everything is *simulated* and every payload carries ``"simulated": true``.  The
demand curve (orders per hour-of-day) and delivery-time distribution come from
:class:`streaming.params.StreamParams` (warehouse-derived or JSON fallback).

Event-time ordering
-------------------
Orders start on a monotonic virtual clock whose rate follows the hour-of-day
demand curve.  Individual order timelines overlap (a delivery spans minutes
while the next order starts seconds later), so the generator merges all
in-flight timelines with a min-heap and emits messages in **event-time order** -
exactly how a healthy stream arrives.  Only deliberately injected faults break
that order, which keeps the watermark and lateness checks meaningful.

Fault injection (configurable ``inject_rates``):

* ``duplicate`` - the exact same message is emitted twice (tests idempotent
  dedup keyed on ``event_id``);
* ``late`` - an event timestamped well before the running watermark;
* ``out_of_bounds`` - coordinates pushed outside the India bounding box;
* ``malformed`` - a payload missing a required field.

The generator is deterministic for a given seed, so tests can assert on exact
output and the recorded run is reproducible.
"""

from __future__ import annotations

import heapq
import json
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from streaming.params import StreamParams
from streaming.validation import to_iso_z

ORDERS_TOPIC = "orders.events"
GPS_TOPIC = "gps.pings"

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
        self._clock = self.base_epoch
        self._seq = 0
        weights = self.params.hour_weights
        self._weights = weights
        self._scale = float(self.params.data.get("daily_orders", 6000)) / sum(weights)

    # -- public --------------------------------------------------------------
    def generate(self, n_events: int) -> Iterator[RawEvent]:
        """Yield exactly ``n_events`` messages (duplicates counted)."""

        emitted = 0
        for event in self._merged_events():
            for raw in self._emit(event):
                yield raw
                emitted += 1
                if emitted >= n_events:
                    return

    # -- event-time merge ----------------------------------------------------
    def _merged_events(self) -> Iterator[dict]:
        """Yield order events in non-decreasing event-time order."""

        heap: list[tuple[float, int, dict]] = []
        next_start = float("-inf")
        while True:
            while not heap or next_start <= heap[0][0]:
                events = self._next_order()
                next_start = events[0]["_ts"]
                for event in events:
                    self._seq += 1
                    heapq.heappush(heap, (event["_ts"], self._seq, event))
            yield heapq.heappop(heap)[2]

    def _next_order(self) -> list[dict]:
        """Generate one order's full timeline on the virtual clock."""

        rng = self.rng
        params = self.params
        self._order_seq += 1
        order_id = f"BLR{self._order_seq:07d}"

        hour = int((self._clock % 86400) // 3600)
        rate_per_hour = max(self._weights[hour] * self._scale, 0.5)
        self._clock += rng.expovariate(rate_per_hour / 3600.0)
        order_ts = self._clock

        vehicle = rng.choices(
            [v for v, _ in self._vehicles], weights=[w for _, w in self._vehicles], k=1
        )[0]
        driver_id = f"DRV-{rng.randrange(params.data['driver_pool']):05d}"
        traffic = _weighted(rng, _TRAFFIC_CHOICES)
        weather = _weighted(rng, _WEATHER_CHOICES)

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

        def make(event_type: str, ts: float, lat, lon, speed) -> dict:
            nonlocal seq
            seq += 1
            return {
                "_ts": ts,
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

        timeline = [
            make("order_created", order_ts, pickup[0], pickup[1], None),
            make("driver_assigned", assigned_ts, pickup[0], pickup[1], params.speed_kmh(vehicle)),
            make("picked_up", pickup_ts, pickup[0], pickup[1], params.speed_kmh(vehicle)),
        ]
        interval = params.ping_interval(vehicle)
        t = pickup_ts + interval
        pings = 0
        while t < drop_ts and pings < _MAX_PINGS_PER_ORDER:
            frac = (t - pickup_ts) / delivery_s if delivery_s else 0.0
            lat = pickup[0] + (drop[0] - pickup[0]) * frac
            lon = pickup[1] + (drop[1] - pickup[1]) * frac
            timeline.append(make("gps_ping", t, lat, lon, params.speed_kmh(vehicle)))
            pings += 1
            t += interval
        timeline.append(make("delivered", drop_ts, drop[0], drop[1], 0.0))
        return timeline

    # -- fault injection + serialisation ------------------------------------
    def _emit(self, event: dict) -> Iterator[RawEvent]:
        event = dict(event)
        ts = event.pop("_ts")
        topic = GPS_TOPIC if event["event_type"] == "gps_ping" else ORDERS_TOPIC
        key = event["driver_id"] if event["event_type"] == "gps_ping" else event["order_id"]

        if event.get("lat") is not None and self.rng.random() < self._rates["out_of_bounds"]:
            event["lat"] = event["lat"] + 55.0
            event["lon"] = event["lon"] + 60.0
            yield RawEvent(topic, key, _dumps(event), event["event_type"], "out_of_bounds")
            return

        injected = None
        if self.rng.random() < self._rates["late"]:
            event["event_ts"] = to_iso_z(ts - _LATE_BACKDATE_S)
            injected = "late"

        if self.rng.random() < self._rates["malformed"]:
            payload = {k: v for k, v in event.items() if k != "event_id"}
            yield RawEvent(topic, key, _dumps(payload), event["event_type"], "malformed")
            return

        value = _dumps(event)
        yield RawEvent(topic, key, value, event["event_type"], injected)

        if self.rng.random() < self._rates["duplicate"]:
            yield RawEvent(topic, key, value, event["event_type"], "duplicate")


def _dumps(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")
