"""JDBC read/write helpers for the Spark jobs.

The warehouse tables are read over JDBC with a column projection and a
``is_valid`` predicate pushed into the SQL, so Spark only pulls the columns the
jobs need.  Marts are written back with ``mode=overwrite`` + ``truncate=true``
as required by docs/CONTRACTS.md section 6; ``isolationLevel=NONE`` is needed
because Postgres runs TRUNCATE outside a transaction (otherwise Spark raises a
"cannot truncate a table referenced in a transaction" error).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark import config


def _read_jdbc(
    spark: SparkSession, table: str, columns: list[str] | None = None, where: str | None = None
) -> DataFrame:
    """Read ``table`` over JDBC, optionally projecting ``columns`` and filtering."""
    select = ", ".join(columns) if columns else "*"
    query = f"(SELECT {select} FROM {table}"
    if where:
        query += f" WHERE {where}"
    query += ") AS src"
    return (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url())
        .option("dbtable", query)
        .option("driver", config.POSTGRES_DRIVER_CLASS)
        .options(**config.jdbc_properties())
        .load()
    )


def read_fct(spark: SparkSession) -> DataFrame:
    """Read valid delivery facts with only the columns the jobs need."""
    return _read_jdbc(spark, config.FCT_TABLE, config.FCT_COLUMNS, where="is_valid")


def read_dim_weather(spark: SparkSession) -> DataFrame:
    """Read the daily weather dimension (rain bucket + rain mm) by city and date."""
    return _read_jdbc(
        spark,
        config.DIM_WEATHER_TABLE,
        ["city", "date", "rain_mm", "rain_bucket"],
    )


def write_mart(df: DataFrame, table: str, num_partitions: int | None = None) -> None:
    """Overwrite a ``spark_marts`` table with ``df`` (truncate, not drop+create)."""
    out = df
    if num_partitions:
        out = out.coalesce(num_partitions)
    (
        out.write.format("jdbc")
        .option("url", config.jdbc_url())
        .option("dbtable", table)
        .option("driver", config.POSTGRES_DRIVER_CLASS)
        .options(**config.jdbc_properties())
        .option("truncate", "true")
        .option("isolationLevel", "NONE")
        .option("batchsize", "1000")
        .mode("overwrite")
        .save()
    )
