"""Mart 1 -- deliveries per pincode per hour (demand heatmap).

Question: *where and when does demand concentrate?*  Operations uses this to
place couriers: each row says how many valid deliveries landed in a pincode in a
given hour, the mean delivery time and the 90th percentile (p90), which shows
the tail that a mean hides.

Grain: one row per (source, pincode, city, hour_of_day).  ``pincode`` is the
drop pincode (the demand point), so deliveries whose drop point did not match a
pincode are excluded -- they cannot be placed on the map.  p90 uses
``percentile_approx`` (Greenwald-Khanna) so it scales to the full dataset.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark import config, io

MART = config.MART_TABLES["pincode_hour"]


def transform(fct: DataFrame, source: str | None = None) -> DataFrame:
    """Aggregate valid facts into the pincode/hour demand mart."""
    df = fct.filter(F.col("is_valid")) if "is_valid" in fct.columns else fct
    if source and source != "all":
        df = df.filter(F.col("source") == source)
    df = df.filter(F.col("drop_pincode").isNotNull())

    return (
        df.groupBy("source", "drop_pincode", "city", "order_hour")
        .agg(
            F.count("*").cast("long").alias("deliveries"),
            F.avg("delivery_minutes").alias("avg_delivery_min"),
            F.percentile_approx("delivery_minutes", 0.9).cast("double").alias("p90_delivery_min"),
        )
        .withColumnRenamed("drop_pincode", "pincode")
        .withColumn("hour_of_day", F.col("order_hour").cast("smallint"))
        .select(
            "source",
            "pincode",
            "city",
            "hour_of_day",
            "deliveries",
            "avg_delivery_min",
            "p90_delivery_min",
        )
    )


def run(
    spark, *, source: str | None = None, fct: DataFrame | None = None, write: bool = True
) -> DataFrame:
    """Read facts (unless supplied), build the mart and optionally persist it."""
    if fct is None:
        fct = io.read_fct(spark)
    result = transform(fct, source=source)
    if write:
        io.write_mart(result, MART)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="deliveries per pincode per hour")
    parser.add_argument("--source", default="all", choices=["all", "amazon", "zomato"])
    parser.add_argument("--master", default=None)
    parser.add_argument("--no-write", action="store_true", help="compute only, do not write")
    parser.add_argument("--sample", action="store_true", help="use the in-memory sample fixture")
    args = parser.parse_args(argv)

    spark = config.build_session("ilm-spark-pincode-hour", args.master)
    try:
        if args.sample:
            from spark import sample

            fct = sample.sample_fct(spark)
        else:
            fct = None
        result = run(spark, source=args.source, fct=fct, write=not args.no_write)
        result.show(20, truncate=False)
        print(f"[pincode_hour] rows={result.count()} wrote={not args.no_write} table={MART}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
