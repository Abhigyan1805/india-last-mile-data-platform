"""Validation, deduplication and event-time watermark logic.

Everything in this module is pure Python with no Kafka or Postgres dependency so
it can be unit-tested on small fixtures offline (see ``tests/streaming``).  The
consumer (``streaming.consumer``) is a thin Kafka/Postgres wrapper around
:class:`StreamProcessor`.

Rules applied to every incoming message, in order:

1. **malformed** - not valid JSON, or missing a required field, or carrying an
   unparseable timestamp / non-numeric coordinate.
2. **duplicate** - ``event_id`` already seen (idempotency key).  The first copy
   is kept; later copies are rejected and counted.
3. **out_of_bounds** - coordinates present but outside the India bounding box
   (used everywhere in the project, see ``docs/CONTRACTS.md``).
4. **late** - event time older than ``watermark - allowed_lateness_s``.  Late
   events are *not* dropped: they still land, flagged ``is_late=true`` so the
   warehouse can tell on-time from late arrivals.

The watermark is the maximum event time seen so far, advanced monotonically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

# India bounding box: lat_min, lat_max, lon_min, lon_max (CONTRACTS section 4).
INDIA_BBOX = (6.0, 37.5, 68.0, 97.5)

VALID_EVENT_TYPES = frozenset(
    {"order_created", "driver_assigned", "picked_up", "delivered", "gps_ping"}
)
VALID_VEHICLES = frozenset({"two_wheeler", "truck"})

REQUIRED_FIELDS = ("event_id", "event_type", "event_ts", "order_id")


def in_india_bbox(lat: float, lon: float) -> bool:
    """True when ``(lat, lon)`` falls inside India's bounding box."""

    lat_min, lat_max, lon_min, lon_max = INDIA_BBOX
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def parse_event_ts(value: object) -> float:
    """Parse an ISO8601 timestamp into epoch seconds (UTC). Raises ValueError."""

    if not isinstance(value, str):
        raise ValueError("event_ts must be a string")
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.timestamp()


def to_iso_z(epoch: float) -> str:
    """Format epoch seconds as compact ISO8601 UTC with a trailing ``Z``."""

    return (
        datetime.fromtimestamp(epoch, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass
class Decision:
    """Outcome of processing one raw message."""

    action: str  # "land" | "reject"
    event: dict | None = None
    reason: str | None = None  # duplicate | out_of_bounds | malformed
    is_late: bool = False

    @property
    def landed(self) -> bool:
        return self.action == "land"


class StreamProcessor:
    """Stateful per-consumer validation: dedup set, watermark and counters."""

    def __init__(self, allowed_lateness_s: float = 300.0) -> None:
        self.allowed_lateness_s = float(allowed_lateness_s)
        self.seen: set[str] = set()
        self.max_event_ts: float = float("-inf")
        # counters (mirrored into raw.stream_runs / quality.check_results)
        self.consumed = 0
        self.landed = 0
        self.duplicates = 0
        self.out_of_bounds = 0
        self.malformed = 0
        self.late = 0
        self.gps_pings = 0
        self.geolocated = 0

    # -- core ----------------------------------------------------------------
    def process(self, raw: bytes | str) -> Decision:
        self.consumed += 1

        event = self._parse(raw)
        if event is None:
            self.malformed += 1
            return Decision(action="reject", reason="malformed")

        event_id = event["event_id"]
        if event_id in self.seen:
            self.duplicates += 1
            return Decision(action="reject", reason="duplicate", event=event)
        self.seen.add(event_id)

        event_ts = event["_event_ts"]
        self.max_event_ts = max(self.max_event_ts, event_ts)

        lat, lon = event.get("lat"), event.get("lon")
        if lat is not None and lon is not None:
            self.geolocated += 1
            if not in_india_bbox(lat, lon):
                self.out_of_bounds += 1
                return Decision(action="reject", reason="out_of_bounds", event=event)

        is_late = event_ts < (self.max_event_ts - self.allowed_lateness_s)
        if is_late:
            self.late += 1
        if event["event_type"] == "gps_ping":
            self.gps_pings += 1
        self.landed += 1
        return Decision(action="land", event=event, is_late=is_late)

    # -- parsing -------------------------------------------------------------
    def _parse(self, raw: bytes | str) -> dict | None:
        try:
            event = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(event, dict):
            return None
        for name in REQUIRED_FIELDS:
            if event.get(name) in (None, ""):
                return None
        if event["event_type"] not in VALID_EVENT_TYPES:
            return None
        vehicle = event.get("vehicle_type")
        if vehicle is not None and vehicle not in VALID_VEHICLES:
            return None
        try:
            event["_event_ts"] = parse_event_ts(event["event_ts"])
        except (ValueError, TypeError):
            return None
        for name in ("lat", "lon", "speed_kmh"):
            value = event.get(name)
            if value is not None and not isinstance(value, (int, float)):
                return None
        return event
