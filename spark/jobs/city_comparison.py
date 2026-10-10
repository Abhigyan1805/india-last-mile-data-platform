"""Mart 4 -- city-by-city comparison of delivery time and volume.

Question: *which cities are hardest to deliver in?*  One row per (source, city)
with volume, mean and p90 delivery time, the share of deliveries over 45
minutes, the mean pickup->drop distance and the pincode match rate (the share of
deliveries whose drop point resolved to a pincode; a low rate means the city's
coordinates were poorly covered by the pincode reference).  The match rate is a
fraction in [0, 1].
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark import config, io

MART = config.MART_TABLES["city_comparison"]
MART_COLUMNS = [
    "source",
    "city",
    "deliveries",
    "avg_delivery_min",
    "p90_delivery_min",
    "pct_over_45_min",
    "avg_distance_km",
    "pincode_match_rate",
]


def transform(fct: DataFrame, source: str | None = None) -> DataFrame:
    """Aggregate valid facts into the per-city comparison mart."""
    df = fct.filter(F.col("is_valid")) if "is_valid" in fct.columns else fct
    if source and source != "all":
        df = df.filter(F.col("source") == source)

    return (
        df.groupBy("source", "city")
        .agg(
            F.count("*").cast("long").alias("deliveries"),
            F.avg("delivery_minutes").alias("avg_delivery_min"),
            F.percentile_approx("delivery_minutes", 0.9).cast("double").alias("p90_delivery_min"),
            (F.lit(100.0) * F.avg((F.col("delivery_minutes") > 45).cast("double"))).alias(
                "pct_over_45_min"
            ),
            F.avg("distance_km").alias("avg_distance_km"),
            F.avg(F.col("drop_pincode").isNotNull().cast("double")).alias("pincode_match_rate"),
        )
        .select(*MART_COLUMNS)
    )


def run(
    spark, *, source: str | None = None, fct: DataFrame | None = None, write: bool = True
) -> DataFrame:
    if fct is None:
        fct = io.read_fct(spark)
    result = transform(fct, source=source)
    if write:
        io.write_mart(result, MART)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="city-by-city delivery comparison")
    parser.add_argument("--source", default="all", choices=["all", "amazon", "zomato"])
    parser.add_argument("--master", default=None)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args(argv)

    spark = config.build_session("ilm-spark-city-comparison", args.master)
    try:
        fct = None
        if args.sample:
            from spark import sample

            fct = sample.sample_fct(spark)
        result = run(spark, source=args.source, fct=fct, write=not args.no_write)
        result.orderBy("source", "city").show(20, truncate=False)
        print(f"[city_comparison] rows={result.count()} wrote={not args.no_write} table={MART}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
