"""Simulation parameters for the streaming piece.

The demand curve (orders per hour-of-day) and the delivery-time distribution
(by traffic and weather) are *learned from the batch warehouse when it exists*
and otherwise fall back to a checked-in JSON file.  See ``docs/CONTRACTS.md``
section 7.

Priority order:

1. ``analytics.*`` star schema produced by the dbt warehouse, or the
   ``spark_marts.*`` aggregates produced by the PySpark jobs, if the tables are
   present and non-empty;
2. the checked-in fallback ``streaming/params_default.json``.

Nothing here writes to the warehouse: this module only reads distributions so
the Kafka simulator emits events whose shape matches the real delivery data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_PARAMS_PATH = Path(__file__).with_name("params_default.json")

# Hour-of-day with the most orders is the busiest slot; the simulator uses the
# weights as a discrete distribution over 24 hours.
_HOURS = 24


@dataclass
class StreamParams:
    """Resolved simulation parameters plus the provenance of each block."""

    data: dict[str, Any]
    sources: dict[str, str] = field(default_factory=dict)

    # -- convenience accessors ------------------------------------------------
    @property
    def city(self) -> str:
        return self.data["city"]

    @property
    def center(self) -> tuple[float, float]:
        return float(self.data["center_lat"]), float(self.data["center_lon"])

    @property
    def spread_deg(self) -> float:
        return float(self.data["spread_deg"])

    @property
    def hour_weights(self) -> list[float]:
        weights = [float(w) for w in self.data["hour_demand_weights"]]
        if len(weights) != _HOURS or sum(weights) <= 0:
            raise ValueError("hour_demand_weights must be 24 positive-ish values")
        return weights

    @property
    def vehicle_mix(self) -> dict[str, float]:
        return {k: float(v) for k, v in self.data["vehicle_mix"].items()}

    @property
    def traffic_base_min(self) -> dict[str, float]:
        return {k: float(v) for k, v in self.data["traffic_base_min"].items()}

    @property
    def weather_multiplier(self) -> dict[str, float]:
        return {k: float(v) for k, v in self.data["weather_multiplier"].items()}

    def ping_interval(self, vehicle: str) -> float:
        return float(self.data["gps_ping_interval_s"][vehicle])

    def speed_kmh(self, vehicle: str) -> float:
        return float(self.data["speed_kmh"][vehicle])

    @property
    def inject_rates(self) -> dict[str, float]:
        return {k: float(v) for k, v in self.data["inject_rates"].items()}

    @property
    def allowed_lateness_s(self) -> float:
        return float(self.data["allowed_lateness_s"])

    # -- loading --------------------------------------------------------------
    @classmethod
    def load(
        cls,
        pg_dsn: str | None = None,
        path: str | Path | None = None,
    ) -> StreamParams:
        """Load parameters, overlaying warehouse-derived distributions.

        ``pg_dsn`` is an optional libpq connection string.  Any failure to reach
        the database, or a missing/empty source table, silently keeps the JSON
        fallback so the simulator always runs.
        """

        base = json.loads(Path(path or DEFAULT_PARAMS_PATH).read_text())
        sources = {"demand": "json", "delivery_time": "json"}
        if pg_dsn:
            overlays = _db_overlays(pg_dsn)
            if overlays.get("hour_demand_weights"):
                base["hour_demand_weights"] = overlays["hour_demand_weights"]
                sources["demand"] = overlays["_demand_source"]
            if overlays.get("traffic_base_min"):
                base["traffic_base_min"] = overlays["traffic_base_min"]
                sources["delivery_time"] = overlays["_delivery_source"]
        return cls(data=base, sources=sources)


def _db_overlays(dsn: str) -> dict[str, Any]:
    """Read demand + traffic delivery times from warehouse tables if present."""

    out: dict[str, Any] = {}
    try:
        import psycopg2
    except Exception:
        return out
    try:
        conn = psycopg2.connect(dsn, connect_timeout=3)
    except Exception:
        return out
    try:
        with conn, conn.cursor() as cur:
            # Hour-of-day demand: prefer the Spark mart, fall back to the dbt fact.
            for table, hour_col in (
                ("spark_marts.deliveries_per_pincode_hour", "hour_of_day"),
                ("analytics.fct_deliveries", "order_hour"),
            ):
                cur.execute(
                    f"SELECT {hour_col}, count(*) FROM {table} "
                    f"WHERE {hour_col} IS NOT NULL GROUP BY 1 ORDER BY 1"
                )
                rows = cur.fetchall()
                if rows:
                    weights = _weights_from_counts(rows)
                    if weights:
                        out["hour_demand_weights"] = weights
                        out["_demand_source"] = table
                        break

            # Delivery time by traffic: use the Spark delay mart when present.
            try:
                cur.execute(
                    "SELECT factor_value, avg_delivery_min FROM spark_marts.delay_impact "
                    "WHERE factor = 'traffic' AND avg_delivery_min IS NOT NULL"
                )
                rows = cur.fetchall()
                if rows:
                    out["traffic_base_min"] = {str(k): float(v) for k, v in rows if v is not None}
                    out["_delivery_source"] = "spark_marts.delay_impact"
            except Exception:
                pass
    except Exception:
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return out


def _weights_from_counts(rows: list[tuple[int, int]]) -> list[float]:
    """Turn (hour, count) rows into 24 zero-or-positive weights."""

    by_hour = {int(h) % 24: float(c) for h, c in rows if h is not None}
    if not by_hour:
        return []
    # Smooth so rare/absent hours keep a non-zero chance of an order.
    total = sum(by_hour.values()) or 1.0
    floor = 0.02 * total / _HOURS
    return [max(by_hour.get(h, 0.0), floor) for h in range(_HOURS)]
