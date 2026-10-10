"""Optional JDBC round-trip integration test (needs a live Postgres).

Skipped unless ``ILM_SPARK_IT=1``.  It uses the ``PGHOST/PGPORT/PGUSER/
PGPASSWORD/PGDATABASE`` env vars, applies the project DDL, loads the sample
fixture into ``analytics.fct_deliveries``, runs the real JDBC path
(``spark.run_all``) and asserts the marts landed in Postgres.  Point it at a
throwaway database -- it truncates the tables it touches::

    PGPORT=5435 PGDATABASE=spark_it ILM_SPARK_IT=1 pytest tests/spark/test_jdbc_integration.py
"""

import os
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

pytestmark = pytest.mark.skipif(
    os.environ.get("ILM_SPARK_IT") != "1",
    reason="set ILM_SPARK_IT=1 with a live Postgres to run the JDBC round-trip",
)

REPO_ROOT = Path(__file__).resolve().parents[2]

FCT_DDL = """
CREATE SCHEMA IF NOT EXISTS analytics;
DROP TABLE IF EXISTS analytics.fct_deliveries;
CREATE TABLE analytics.fct_deliveries (
  source text, order_id text, courier_id text, city text,
  pickup_pincode text, drop_pincode text,
  order_ts timestamp, order_date date, order_hour smallint,
  delivery_minutes numeric, distance_km double precision,
  weather text, traffic text, festival boolean, rain_mm double precision,
  is_valid boolean
);
DROP TABLE IF EXISTS analytics.dim_weather_daily;
CREATE TABLE analytics.dim_weather_daily (
  city text, date date, rain_mm double precision, rain_bucket text
);
"""

FCT_COLS = [
    "source",
    "order_id",
    "courier_id",
    "city",
    "pickup_pincode",
    "drop_pincode",
    "order_ts",
    "order_date",
    "order_hour",
    "delivery_minutes",
    "distance_km",
    "weather",
    "traffic",
    "festival",
    "rain_mm",
    "is_valid",
]
DIM_COLS = ["city", "date", "rain_mm", "rain_bucket"]


def _connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5433"),
        user=os.environ.get("PGUSER", "ilm"),
        password=os.environ.get("PGPASSWORD", "ilm"),
        dbname=os.environ.get("PGDATABASE", "warehouse"),
    )


def _load_fixture(rows, weather_rows):
    ddl = (REPO_ROOT / "ingest" / "sql" / "ddl.sql").read_text()
    placeholders = ", ".join(["%s"] * len(FCT_COLS))
    dim_placeholders = ", ".join(["%s"] * len(DIM_COLS))
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(FCT_DDL)
        cur.executemany(
            f"INSERT INTO analytics.fct_deliveries ({', '.join(FCT_COLS)}) VALUES ({placeholders})",
            [tuple(r[c] for c in FCT_COLS) for r in rows],
        )
        cur.executemany(
            f"INSERT INTO analytics.dim_weather_daily ({', '.join(DIM_COLS)}) "
            f"VALUES ({dim_placeholders})",
            [tuple(r[c] for c in DIM_COLS) for r in weather_rows],
        )


def _count(table):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def test_jdbc_read_transform_write(spark):
    from spark import run_all, sample

    _load_fixture(sample.sample_fct(spark).collect(), sample.sample_weather(spark).collect())

    counts = run_all.run_all(spark, write=True)

    assert counts == {
        "pincode_hour": 7,
        "courier_utilisation": 2,
        "delay_impact": 20,
        "city_comparison": 5,
    }
    # Prove the rows physically landed in Postgres.
    assert _count("spark_marts.deliveries_per_pincode_hour") == 7
    assert _count("spark_marts.courier_utilisation") == 2
    assert _count("spark_marts.delay_impact") == 20
    assert _count("spark_marts.city_comparison") == 5
