"""PostgreSQL landing layer for the streaming consumer.

Lands validated events into ``raw.stream_events`` (PK on ``event_id`` so the
upsert is idempotent across re-runs), rejects into ``raw.stream_rejects``,
per-run totals into ``raw.stream_runs`` and stream quality counts into
``quality.check_results``.  Table definitions match ``ingest/sql/ddl.sql`` and
``docs/CONTRACTS.md``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from psycopg2.extras import execute_values

DDL_PATH = Path(__file__).with_name("sql").joinpath("stream_ddl.sql")

# quality.check_results rows written per streaming run (CONTRACTS section 5).
STREAM_CHECKS = (
    "out_of_bounds_gps",
    "duplicate_event",
    "late_arriving_ping",
    "malformed_event",
)


def dsn_from_env() -> str:
    """Build a libpq conninfo from the contract PG* environment variables."""

    return (
        f"host={os.environ.get('PGHOST', '127.0.0.1')} "
        f"port={os.environ.get('PGPORT', '5433')} "
        f"user={os.environ.get('PGUSER', 'ilm')} "
        f"password={os.environ.get('PGPASSWORD', 'ilm')} "
        f"dbname={os.environ.get('PGDATABASE', 'warehouse')}"
    )


def connect(dsn: str | None = None):
    """Open a psycopg2 connection using ``dsn`` or the environment."""

    import psycopg2

    return psycopg2.connect(dsn or dsn_from_env(), connect_timeout=5)


def ensure_schema(conn) -> None:
    """Create the streaming tables if they do not exist (idempotent)."""

    with conn, conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text())


class StreamSink:
    """Batched writer for landed events and rejects with a final run summary."""

    def __init__(self, conn, load_id: str, batch_size: int = 1000) -> None:
        self.conn = conn
        self.load_id = load_id
        self.batch_size = batch_size
        self._events: list[tuple] = []
        self._rejects: list[tuple] = []
        self.landed_written = 0

    def land(self, event: dict, is_late: bool, received_epoch: float) -> None:
        self._events.append(
            (
                event.get("event_id"),
                event.get("event_type"),
                event.get("order_id"),
                event.get("driver_id"),
                event.get("vehicle_type"),
                event.get("city"),
                event.get("event_ts"),
                _epoch_to_ts(received_epoch),
                event.get("lat"),
                event.get("lon"),
                event.get("speed_kmh"),
                bool(is_late),
                json.dumps(event, separators=(",", ":")),
                self.load_id,
            )
        )
        if len(self._events) >= self.batch_size:
            self._flush_events()

    def reject(self, event: dict | None, reason: str, received_epoch: float) -> None:
        self._rejects.append(
            (
                (event or {}).get("event_id"),
                reason,
                json.dumps(event, separators=(",", ":")),
                _epoch_to_ts(received_epoch),
                self.load_id,
            )
        )
        if len(self._rejects) >= self.batch_size:
            self._flush_rejects()

    # -- flushing ------------------------------------------------------------
    def _flush_events(self) -> None:
        if not self._events:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO raw.stream_events (event_id, event_type, order_id, driver_id, "
                "vehicle_type, city, event_ts, received_ts, latitude, longitude, speed_kmh, "
                "is_late, payload, _load_id) VALUES %s "
                "ON CONFLICT (event_id) DO NOTHING",
                self._events,
                page_size=self.batch_size,
            )
        self.conn.commit()
        self.landed_written += len(self._events)
        self._events.clear()

    def _flush_rejects(self) -> None:
        if not self._rejects:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO raw.stream_rejects (event_id, reason, payload, received_ts, "
                "_load_id) VALUES %s",
                self._rejects,
                page_size=self.batch_size,
            )
        self.conn.commit()
        self._rejects.clear()

    def close(self) -> None:
        self._flush_events()
        self._flush_rejects()

    # -- summaries -----------------------------------------------------------
    def write_run(self, stats: dict) -> None:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.stream_runs (load_id, produced, consumed, landed, "
                "duplicates_dropped, late_events, out_of_bounds, malformed, duration_s, "
                "events_per_s) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (load_id) DO UPDATE SET produced=EXCLUDED.produced, "
                "consumed=EXCLUDED.consumed, landed=EXCLUDED.landed, "
                "duplicates_dropped=EXCLUDED.duplicates_dropped, "
                "late_events=EXCLUDED.late_events, out_of_bounds=EXCLUDED.out_of_bounds, "
                "malformed=EXCLUDED.malformed, duration_s=EXCLUDED.duration_s, "
                "events_per_s=EXCLUDED.events_per_s",
                (
                    self.load_id,
                    stats["produced"],
                    stats["consumed"],
                    stats["landed"],
                    stats["duplicates"],
                    stats["late"],
                    stats["out_of_bounds"],
                    stats["malformed"],
                    stats["duration_s"],
                    stats["events_per_s"],
                ),
            )

    def write_quality(self, stats: dict) -> None:
        """Write the minimum stream checks (CONTRACTS section 5)."""

        gps_checked = stats["gps_pings"]
        rows = [
            ("out_of_bounds_gps", "stream", stats["geolocated"], stats["out_of_bounds"]),
            ("duplicate_event", "stream", stats["consumed"], stats["duplicates"]),
            ("late_arriving_ping", "gps.pings", gps_checked, stats["late"]),
            ("malformed_event", "stream", stats["consumed"], stats["malformed"]),
        ]
        with self.conn, self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO quality.check_results (run_id, layer, check_name, dataset, "
                "rows_checked, rows_failed, details) VALUES %s "
                "ON CONFLICT (run_id, check_name, dataset) DO UPDATE SET "
                "rows_checked=EXCLUDED.rows_checked, rows_failed=EXCLUDED.rows_failed, "
                "details=EXCLUDED.details, run_ts=now()",
                [
                    (
                        self.load_id,
                        "stream",
                        name,
                        dataset,
                        checked,
                        failed,
                        json.dumps({"reject_reason": _check_reason(name)}),
                    )
                    for name, dataset, checked, failed in rows
                ],
                page_size=10,
            )


def _check_reason(check_name: str) -> str:
    return {
        "out_of_bounds_gps": "out_of_bounds",
        "duplicate_event": "duplicate",
        "late_arriving_ping": "late",
        "malformed_event": "malformed",
    }[check_name]


def _epoch_to_ts(epoch: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch, tz=UTC).isoformat()
