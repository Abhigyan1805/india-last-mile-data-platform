"""End-to-end check of the dbt warehouse on the tiny fixture (warehouse/sample/*.csv).

Needs a running PostgreSQL (see docs/CONTRACTS.md; export PGPORT if it is not 5433) and dbt-postgres;
skipped otherwise. Uses its own database `warehouse_sample`, never the real `warehouse`.
The fixture plants known bad rows, so the exact failing-row counts below are asserted.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[2]
ENV = {
    **os.environ,
    "PGDATABASE": "warehouse_sample",
    "DBT_PROFILES_DIR": str(ROOT / "warehouse"),
    "LOAD_ID": "TEST",
}


def _dbt(*args: str) -> subprocess.CompletedProcess:
    exe = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")
    return subprocess.run(
        [exe, *args], cwd=ROOT / "warehouse", env=ENV, capture_output=True, text=True
    )


def _connect():
    return psycopg2.connect(
        host=ENV.get("PGHOST", "127.0.0.1"),
        port=ENV.get("PGPORT", "5433"),
        user=ENV.get("PGUSER", "ilm"),
        password=ENV.get("PGPASSWORD", "ilm"),
        dbname="warehouse_sample",
    )


@pytest.fixture(scope="module")
def built():
    if not shutil.which("dbt", path=str(Path(sys.executable).parent)) and not shutil.which("dbt"):
        pytest.skip("dbt not installed")
    try:
        psycopg2.connect(
            host=ENV.get("PGHOST", "127.0.0.1"),
            port=ENV.get("PGPORT", "5433"),
            user=ENV.get("PGUSER", "ilm"),
            password=ENV.get("PGPASSWORD", "ilm"),
            dbname="postgres",
        ).close()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not reachable")
    os.environ.update({k: ENV[k] for k in ("PGDATABASE", "LOAD_ID")})
    from warehouse import load_sample, quality

    load_sample.load()
    res = _dbt("build", "--full-refresh")
    assert res.returncode == 0, res.stdout[-3000:]
    quality.run()
    return res


def _rows(sql: str):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_dbt_build_passes_with_dq_warnings(built):
    # hard tests pass; the four dq_* singular tests warn because the fixture contains bad rows
    assert "ERROR=0" in built.stdout and "WARN=4" in built.stdout


def test_star_schema_contents(built):
    assert _rows("select count(*), count(distinct delivery_key) from analytics.fct_deliveries") == [
        (27, 27)
    ]
    assert _rows("select count(*) from analytics.fct_deliveries where is_valid") == [(18,)]
    assert _rows("select count(*) from analytics.dim_time") == [(24,)]
    assert _rows(
        "select rain_mm from analytics.fct_deliveries where delivery_key = 'amazon-A003'"
    ) == [(12.0,)]
    assert _rows("select count(*) from analytics.dim_date where is_festival_season") == [(5,)]


def test_quality_results_counts(built):
    got = dict(
        ((r[0], r[1]), r[2])
        for r in _rows(
            "select check_name, dataset, rows_failed from quality.check_results where run_id = 'TEST'"
        )
    )
    expected = {
        ("coords_outside_india", "amazon"): 1,
        ("coords_outside_india", "zomato"): 1,
        ("negative_or_absurd_delivery_time", "amazon"): 2,
        ("negative_or_absurd_delivery_time", "zomato"): 1,
        ("duplicate_order_id", "amazon"): 1,
        ("duplicate_order_id", "zomato"): 1,
        ("unparseable_timestamp", "zomato"): 2,
        ("null_required_field", "zomato"): 1,
        ("pincode_unmatched", "amazon"): 2,
        ("pincode_unmatched", "zomato"): 1,
        ("orphan_pincode", "amazon"): 1,
        ("fct_rows_marked_invalid", "fct_deliveries"): 9,
    }
    for key, n in expected.items():
        assert got[key] == n, key


def test_incremental_rerun_is_idempotent(built):
    res = _dbt("build", "--select", "fct_deliveries")
    assert res.returncode == 0, res.stdout[-2000:]
    assert _rows("select count(*), count(distinct delivery_key) from analytics.fct_deliveries") == [
        (27, 27)
    ]
    from warehouse import quality

    quality.run()  # same run_id -> overwritten, not duplicated
    assert _rows(
        "select count(*) from quality.check_results where run_id = 'TEST' and check_name = 'duplicate_order_id'"
    ) == [(2,)]
