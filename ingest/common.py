"""Shared settings and helpers for the ingest package."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = ROOT / "ingest" / "sql" / "ddl.sql"
INDIA_BBOX = (6.0, 37.5, 68.0, 97.5)  # lat_min, lat_max, lon_min, lon_max


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    sample_mode: bool
    demo_rows: int
    geo_match_max_km: float
    load_id: str
    partition_date: str | None  # ISO date; when set, load/geo only that order_date partition

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", ROOT / "data")),
            sample_mode=os.environ.get("SAMPLE_MODE", "0") == "1",
            demo_rows=int(os.environ.get("DEMO_ROWS", "2000")),
            geo_match_max_km=float(os.environ.get("GEO_MATCH_MAX_KM", "10")),
            load_id=os.environ.get("LOAD_ID") or datetime.now().strftime("%Y%m%dT%H%M%S"),
            partition_date=os.environ.get("PARTITION_DATE") or None,
        )


def connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "5433")),
        user=os.environ.get("PGUSER", "ilm"),
        password=os.environ.get("PGPASSWORD", "ilm"),
        dbname=os.environ.get("PGDATABASE", "warehouse"),
    )


def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text())
    conn.commit()
