"""Deterministic in-memory sample fixture.

Lets every job (and the test suite) run end-to-end without Postgres or the full
Kaggle dataset.  The rows are hand-built so the expected aggregates are known:
two sources, three cities, matched and unmatched drop pincodes, one invalid
Amazon row, couriers with single- and multi-order days, and rain buckets via the
weather dimension.  ``python -m spark.run_all --sample`` exercises all four jobs.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

FCT_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("order_id", StringType()),
        StructField("courier_id", StringType()),
        StructField("city", StringType()),
        StructField("pickup_pincode", StringType()),
        StructField("drop_pincode", StringType()),
        StructField("order_ts", TimestampType()),
        StructField("order_date", DateType()),
        StructField("order_hour", IntegerType()),
        StructField("delivery_minutes", DoubleType()),
        StructField("distance_km", DoubleType()),
        StructField("weather", StringType()),
        StructField("traffic", StringType()),
        StructField("festival", BooleanType()),
        StructField("rain_mm", DoubleType()),
        StructField("is_valid", BooleanType()),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("date", DateType()),
        StructField("rain_mm", DoubleType()),
        StructField("rain_bucket", StringType()),
    ]
)


def _ts(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi)


def sample_fct(spark: SparkSession) -> DataFrame:
    """Return the tiny delivery-facts fixture."""
    d1, d2 = date(2026, 9, 1), date(2026, 9, 2)
    rows = [
        # --- Amazon: no courier id, no festival ---------------------------------
        (
            "amazon",
            "a1",
            None,
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 1, 8, 15),
            d1,
            8,
            25.0,
            4.5,
            "Sunny",
            "Low",
            None,
            0.0,
            True,
        ),
        (
            "amazon",
            "a2",
            None,
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 1, 8, 45),
            d1,
            8,
            35.0,
            5.0,
            "Storm",
            "High",
            None,
            0.0,
            True,
        ),
        (
            "amazon",
            "a3",
            None,
            "Mumbai",
            "400001",
            "400050",
            _ts(2026, 9, 1, 18, 30),
            d1,
            18,
            50.0,
            12.0,
            "Cloudy",
            "Medium",
            None,
            0.0,
            True,
        ),
        (
            "amazon",
            "a4",
            None,
            "Mumbai",
            "400001",
            None,
            _ts(2026, 9, 1, 19, 0),
            d1,
            19,
            40.0,
            3.0,
            "Cloudy",
            "Medium",
            None,
            0.0,
            True,
        ),
        (
            "amazon",
            "a5",
            None,
            "Delhi",
            "110001",
            "110020",
            _ts(2026, 9, 2, 12, 0),
            d2,
            12,
            20.0,
            8.0,
            "Fog",
            "Low",
            None,
            0.0,
            True,
        ),
        (
            "amazon",
            "a6",
            None,
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 2, 9, 0),
            d2,
            9,
            300.0,
            4.0,
            "Sunny",
            "Low",
            None,
            0.0,
            False,
        ),
        # --- Zomato: couriers, festival flag ------------------------------------
        (
            "zomato",
            "z1",
            "DEHRES001",
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 1, 8, 0),
            d1,
            8,
            20.0,
            3.0,
            "Sunny",
            "Low",
            False,
            0.0,
            True,
        ),
        (
            "zomato",
            "z2",
            "DEHRES001",
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 1, 8, 30),
            d1,
            8,
            25.0,
            3.5,
            "Sunny",
            "Low",
            False,
            0.0,
            True,
        ),
        (
            "zomato",
            "z3",
            "DEHRES001",
            "Bengaluru",
            "560001",
            "560034",
            _ts(2026, 9, 1, 9, 0),
            d1,
            9,
            30.0,
            4.0,
            "Cloudy",
            "Medium",
            False,
            0.0,
            True,
        ),
        (
            "zomato",
            "z4",
            "DEHRES002",
            "Mumbai",
            "400001",
            "400050",
            _ts(2026, 9, 1, 18, 0),
            d1,
            18,
            60.0,
            10.0,
            "Storm",
            "High",
            True,
            0.0,
            True,
        ),
        (
            "zomato",
            "z5",
            "DEHRES002",
            "Mumbai",
            "400001",
            "400050",
            _ts(2026, 9, 1, 19, 0),
            d1,
            19,
            40.0,
            10.0,
            "Storm",
            "High",
            True,
            0.0,
            True,
        ),
    ]
    return spark.createDataFrame(rows, FCT_SCHEMA)


def sample_weather(spark: SparkSession) -> DataFrame:
    """Return the tiny daily-weather dimension fixture."""
    rows = [
        ("Bengaluru", date(2026, 9, 1), 0.0, "none"),
        ("Bengaluru", date(2026, 9, 2), 5.0, "light"),
        ("Mumbai", date(2026, 9, 1), 30.0, "heavy"),
        ("Delhi", date(2026, 9, 2), 12.0, "moderate"),
    ]
    return spark.createDataFrame(rows, WEATHER_SCHEMA)
