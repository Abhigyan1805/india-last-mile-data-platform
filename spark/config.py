"""Spark runtime configuration and session factory.

All Spark settings are centralised here so the four jobs and the tests build an
identical session.  The design keeps the driver small (shared 5 GB machine):
driver heap <= 1g, executor heap <= 512m, a handful of shuffle partitions and
the Spark UI off.  Python workers are pinned to the interpreter that launched
the driver -- on this machine the system Python (3.14) differs from the venv
Python (3.12) and PySpark refuses to mix them.

Environment (see docs/CONTRACTS.md section 2 and scripts/env.sh):
    PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE   Postgres connection
    SPARK_MASTER                                 Spark master, default local[*]
    POSTGRES_JDBC_JAR                            optional explicit driver jar
"""

from __future__ import annotations

import os
import sys
from glob import glob
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JAR_DIR = REPO_ROOT / ".tools" / "jars"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.7.3"
POSTGRES_DRIVER_CLASS = "org.postgresql.Driver"

# --- schemas / tables (must match ingest/sql/ddl.sql) ----------------------
FCT_TABLE = "analytics.fct_deliveries"
DIM_WEATHER_TABLE = "analytics.dim_weather_daily"

MART_TABLES = {
    "pincode_hour": "spark_marts.deliveries_per_pincode_hour",
    "courier_utilisation": "spark_marts.courier_utilisation",
    "delay_impact": "spark_marts.delay_impact",
    "city_comparison": "spark_marts.city_comparison",
}

# Columns the jobs need from analytics.fct_deliveries.
FCT_COLUMNS = [
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


def pg_env() -> dict[str, str]:
    """Postgres connection settings with the same defaults as scripts/env.sh."""
    return {
        "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": os.environ.get("PGPORT", "5433"),
        "user": os.environ.get("PGUSER", "ilm"),
        "password": os.environ.get("PGPASSWORD", "ilm"),
        "database": os.environ.get("PGDATABASE", "warehouse"),
    }


def jdbc_url() -> str:
    e = pg_env()
    return f"jdbc:postgresql://{e['host']}:{e['port']}/{e['database']}"


def jdbc_properties() -> dict[str, str]:
    e = pg_env()
    return {"user": e["user"], "password": e["password"], "driver": POSTGRES_DRIVER_CLASS}


def postgres_jar() -> str | None:
    """Return a local Postgres JDBC jar path, or None if only a Maven coord is available."""
    explicit = os.environ.get("POSTGRES_JDBC_JAR")
    if explicit and Path(explicit).exists():
        return explicit
    candidates = sorted(glob(str(DEFAULT_JAR_DIR / "postgresql-*.jar")))
    return candidates[-1] if candidates else None


def build_session(app_name: str = "ilm-spark", master: str | None = None):
    """Build (or fetch) a SparkSession configured for this machine."""
    from pyspark.sql import SparkSession

    # Pin Python workers to the driver interpreter (avoids 3.14-vs-3.12 mismatch).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(REPO_ROOT / "spark-warehouse"))
        .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"])
        .config("spark.pyspark.driver.python", os.environ["PYSPARK_DRIVER_PYTHON"])
    )

    jar = postgres_jar()
    if jar:
        builder = builder.config("spark.jars", jar)
    else:
        # No local jar: let Ivy fetch it (needs network, used only as a fallback).
        builder = builder.config("spark.jars.packages", POSTGRES_JDBC_COORD)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
