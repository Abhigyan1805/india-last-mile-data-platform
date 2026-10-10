"""Batch data-quality runner: counts failing rows per check and records them in quality.check_results.

The rules themselves live in dbt (`int_deliveries` flags, built from macros/dq.sql). This module just counts the
flags per source and writes one row per (run_id, check_name, dataset) with rows_failed / rows_checked and a
run timestamp, so the README can quote real "bad rows caught" numbers. Re-running with the same run_id
overwrites that run's rows (idempotent); a new run_id (the pipeline LOAD_ID) adds a new set of rows.

Usage: python -m warehouse.quality            (run_id = $LOAD_ID, else latest raw.ingest_runs.load_id, else a timestamp)
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import psycopg2

LAYER = "batch"

# check_name -> boolean column on staging.int_deliveries that is TRUE for a failing row
DELIVERY_CHECKS = {
    "coords_outside_india": "coords_outside_india",
    "negative_or_absurd_delivery_time": "bad_delivery_time",
    "duplicate_order_id": "is_duplicate",
    "null_required_field": "missing_delivery_time",
    "unparseable_timestamp": "unparseable_timestamp",
    "pincode_unmatched": "pincode_unmatched",
    "orphan_pincode": "orphan_pincode",
}

DDL = """
create schema if not exists quality;
create table if not exists quality.check_results (
  run_id text not null, run_ts timestamptz not null default now(),
  layer text not null, check_name text not null, dataset text not null,
  rows_checked bigint, rows_failed bigint not null, details jsonb,
  primary key (run_id, check_name, dataset));
"""


def connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5433"),
        user=os.environ.get("PGUSER", "ilm"),
        password=os.environ.get("PGPASSWORD", "ilm"),
        dbname=os.environ.get("PGDATABASE", "warehouse"),
    )


def resolve_run_id(cur) -> str:
    if os.environ.get("LOAD_ID"):
        return os.environ["LOAD_ID"]
    cur.execute("select to_regclass('raw.ingest_runs')")
    if cur.fetchone()[0]:
        cur.execute(
            "select load_id from raw.ingest_runs order by started_at desc nulls last limit 1"
        )
        row = cur.fetchone()
        if row:
            return row[0]
    return datetime.now().strftime("%Y%m%dT%H%M%S")


def collect(cur) -> list[tuple[str, str, int, int, dict]]:
    """Return (check_name, dataset, rows_checked, rows_failed, details) for every check."""
    results = []
    flags = ", ".join(
        f"count(*) filter (where {col}) as {name}" for name, col in DELIVERY_CHECKS.items()
    )
    cur.execute(
        f"select source, count(*), {flags} from staging.int_deliveries group by source order by source"
    )
    for source, checked, *failed in cur.fetchall():
        for (name, _), n in zip(DELIVERY_CHECKS.items(), failed, strict=True):
            results.append((name, source, checked, n, {"table": "staging.int_deliveries"}))
    # source-level check: rows dropped before the warehouse because order_id is missing
    for source, table in (("amazon", "stg_amazon_deliveries"), ("zomato", "stg_zomato_deliveries")):
        cur.execute(
            f"select count(*), count(*) filter (where order_id is null) from staging.{table}"
        )
        checked, n = cur.fetchone()
        results.append(("null_order_id", source, checked, n, {"table": f"staging.{table}"}))
    # final-table consistency: invalid rows in the star schema
    cur.execute(
        "select count(*), count(*) filter (where not is_valid) from analytics.fct_deliveries"
    )
    checked, n = cur.fetchone()
    results.append(
        (
            "fct_rows_marked_invalid",
            "fct_deliveries",
            checked,
            n,
            {"table": "analytics.fct_deliveries"},
        )
    )
    return results


def run() -> str:
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        run_id = resolve_run_id(cur)
        for name, dataset, checked, failed, details in collect(cur):
            cur.execute(
                """insert into quality.check_results
                     (run_id, run_ts, layer, check_name, dataset, rows_checked, rows_failed, details)
                   values (%s, now(), %s, %s, %s, %s, %s, %s)
                   on conflict (run_id, check_name, dataset) do update set
                     run_ts = now(), rows_checked = excluded.rows_checked,
                     rows_failed = excluded.rows_failed, details = excluded.details""",
                (run_id, LAYER, name, dataset, checked, failed, json.dumps(details)),
            )
            print(f"{name:36s} {dataset:16s} failed {failed:>8} / {checked}")
    conn.close()
    return run_id


if __name__ == "__main__":
    print("run_id:", run())
