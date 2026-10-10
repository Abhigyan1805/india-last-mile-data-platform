"""Load raw CSVs into Postgres raw.* (TEXT landing). Idempotent: re-running replaces, never doubles.

With PARTITION_DATE=YYYY-MM-DD only that order_date partition of the two delivery sources is
replaced (Airflow backfill); pincodes are always replaced wholesale.
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
from psycopg2.extensions import connection

from ingest.common import Settings, connect, ensure_schema

SOURCES = {
    # table: (csv path under raw/, date column, strptime format of that column)
    "amazon_deliveries": ("amazon/amazon_delivery.csv", "order_date", "%Y-%m-%d"),
    "zomato_deliveries": ("zomato/Zomato Dataset.csv", "order_date", "%d-%m-%Y"),
    "pincodes": ("pincodes/all_india_pincode_directory_2025.csv", None, None),
}
RENAMES = {"time_taken (min)": "time_taken_min"}


def read_source(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [RENAMES.get(c.strip().lower(), c.strip().lower()) for c in df.columns]
    return df


def sample(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return df.sample(n=min(n, len(df)), random_state=42).sort_index()


def copy_df(conn: connection, table: str, df: pd.DataFrame, load_id: str) -> None:
    df = df.assign(_load_id=load_id)
    cols = ", ".join(df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY raw.{table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)


def load_table(conn: connection, table: str, settings: Settings) -> int:
    rel, date_col, fmt = SOURCES[table]
    df = read_source(settings.data_dir / "raw" / rel)
    if settings.sample_mode and table != "pincodes":
        df = sample(df, settings.demo_rows)
    with conn.cursor() as cur:
        if settings.partition_date and date_col:
            day = datetime.strptime(settings.partition_date, "%Y-%m-%d").strftime(fmt)
            df = df[df[date_col] == day]
            cur.execute(f"DELETE FROM raw.{table} WHERE {date_col} = %s", (day,))
        else:
            cur.execute(f"TRUNCATE raw.{table}")
    copy_df(conn, table, df, settings.load_id)
    print(f"[load] raw.{table}: {len(df)} rows")
    return len(df)


def main() -> None:
    settings = Settings.from_env()
    conn = connect()
    ensure_schema(conn)
    started = datetime.now()
    counts = {t: load_table(conn, t, settings) for t in SOURCES}
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw.ingest_runs (load_id, started_at, finished_at, sample_mode,
                   rows_amazon, rows_zomato, rows_pincodes)
               VALUES (%s,%s,now(),%s,%s,%s,%s)
               ON CONFLICT (load_id) DO UPDATE SET finished_at = now(),
                   rows_amazon = EXCLUDED.rows_amazon, rows_zomato = EXCLUDED.rows_zomato,
                   rows_pincodes = EXCLUDED.rows_pincodes""",
            (settings.load_id, started, settings.sample_mode, counts["amazon_deliveries"],
             counts["zomato_deliveries"], counts["pincodes"]),
        )
    conn.commit()


if __name__ == "__main__":
    main()
