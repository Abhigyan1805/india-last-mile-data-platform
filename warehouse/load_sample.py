"""Load the tiny sample fixture (warehouse/sample/*.csv) into raw.* of a SEPARATE database.

Why a separate database: `make warehouse-sample` must never touch the real `warehouse` database, so the
fixture lives in `warehouse_sample` (same Postgres server). The fixture contains planted bad rows (duplicates,
(0,0) and negative coordinates, absurd delivery times, NaN values, an orphan pincode) so tests can assert exact
data-quality counts.  Usage: PGDATABASE=warehouse_sample python -m warehouse.load_sample
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = Path(__file__).resolve().parent / "sample"
SAMPLE_DB = "warehouse_sample"
LOAD_ID = "SAMPLE"

# csv file -> raw table (CSV header = column names)
TABLES = {
    "amazon_deliveries": "amazon_deliveries",
    "zomato_deliveries": "zomato_deliveries",
    "pincodes": "pincodes",
    "delivery_geo": "delivery_geo",
    "city_ref": "city_ref",
    "weather_daily": "weather_daily",
}
LOAD_ID_TABLES = {
    "amazon_deliveries",
    "zomato_deliveries",
    "pincodes",
    "delivery_geo",
    "weather_daily",
}


def connect(dbname: str):
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5433"),
        user=os.environ.get("PGUSER", "ilm"),
        password=os.environ.get("PGPASSWORD", "ilm"),
        dbname=dbname,
    )


def ensure_database(dbname: str = SAMPLE_DB) -> None:
    conn = connect("postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("select 1 from pg_database where datname = %s", (dbname,))
        if not cur.fetchone():
            cur.execute(sql.SQL("create database {}").format(sql.Identifier(dbname)))
    conn.close()


def load(dbname: str = SAMPLE_DB) -> dict[str, int]:
    """Create schemas (ingest DDL), replace raw rows with the fixture, return row counts per table."""
    ensure_database(dbname)
    counts: dict[str, int] = {}
    conn = connect(dbname)
    with conn, conn.cursor() as cur:
        cur.execute((ROOT / "ingest" / "sql" / "ddl.sql").read_text())
        for name, table in TABLES.items():
            cur.execute(sql.SQL("truncate raw.{}").format(sql.Identifier(table)))
            path = SAMPLE_DIR / f"{name}.csv"
            header = path.read_text().splitlines()[0].split(",")
            cols = header + (["_load_id"] if table in LOAD_ID_TABLES else [])
            # stage into a temp table so _load_id can be added with a constant
            cur.execute(
                sql.SQL(
                    "create temp table _s (like raw.{} including defaults) on commit drop"
                ).format(sql.Identifier(table))
            )
            if table in LOAD_ID_TABLES:
                cur.execute("alter table _s alter column _load_id drop not null")
            with path.open() as f:
                cur.copy_expert(
                    sql.SQL("copy _s ({}) from stdin with (format csv, header true, null '')")
                    .format(sql.SQL(", ").join(sql.Identifier(c) for c in header))
                    .as_string(conn),
                    f,
                )
            if table in LOAD_ID_TABLES:
                cur.execute("update _s set _load_id = %s", (LOAD_ID,))
            ident = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
            cur.execute(
                sql.SQL("insert into raw.{} ({}) select {} from _s").format(
                    sql.Identifier(table), ident, ident
                )
            )
            cur.execute(sql.SQL("select count(*) from raw.{}").format(sql.Identifier(table)))
            counts[table] = cur.fetchone()[0]
            cur.execute("drop table _s")
    conn.close()
    return counts


if __name__ == "__main__":
    print(load(os.environ.get("PGDATABASE", SAMPLE_DB)))
