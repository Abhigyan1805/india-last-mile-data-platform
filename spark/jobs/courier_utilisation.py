"""Mart 2 -- courier utilisation (Zomato only).

Question: *how busy are couriers, and how much of a shift is spent actually
delivering?*  Utilisation is the share of on-shift time spent on a delivery,
capped at 1 (a courier can hand over the next order before the previous
``Time_taken`` is over, which can push busy above on-shift).

Definitions follow docs/CONTRACTS.md section 6:
  * busy minutes  = sum of ``delivery_minutes`` for the courier;
  * on-shift time = per courier-*day*, ``last order_ts + delivery - first
    order_ts``, summed across that courier's active days;
  * utilisation   = busy / on-shift, capped at 1.

Amazon rows have no courier id, so this mart is Zomato-only.  A courier that
worked in several cities is reported under the city where they delivered most.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark import config, io

MART = config.MART_TABLES["courier_utilisation"]


def transform(fct: DataFrame, source: str | None = None) -> DataFrame:
    """Aggregate valid Zomato facts into the courier utilisation mart."""
    df = fct.filter(F.col("is_valid")) if "is_valid" in fct.columns else fct
    df = df.filter((F.col("source") == "zomato") & F.col("courier_id").isNotNull())

    base = df.withColumn("start_epoch", F.unix_timestamp("order_ts")).withColumn(
        "end_epoch", F.col("start_epoch") + F.col("delivery_minutes") * 60
    )

    # On-shift time is measured per courier-day, then summed over active days.
    per_day = base.groupBy("courier_id", "order_date").agg(
        F.min("start_epoch").alias("day_start"),
        F.max("end_epoch").alias("day_end"),
        F.sum("delivery_minutes").alias("busy_day"),
        F.count("*").cast("long").alias("deliv_day"),
    )
    per_courier = per_day.groupBy("courier_id").agg(
        F.count("*").cast("int").alias("active_days"),
        F.sum("deliv_day").cast("long").alias("deliveries"),
        F.sum("busy_day").alias("busy_minutes"),
        (F.sum(F.col("day_end") - F.col("day_start")) / F.lit(60.0)).alias("on_shift_minutes"),
    )

    # Dominant city per courier (most deliveries; ties broken by name for determinism).
    window = Window.partitionBy("courier_id").orderBy(F.col("n").desc(), F.col("city").asc())
    city_mode = (
        base.groupBy("courier_id", "city")
        .agg(F.count("*").alias("n"))
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select("courier_id", "city")
    )

    joined = per_courier.join(F.broadcast(city_mode), on="courier_id", how="left")
    return (
        joined.withColumn("source", F.lit("zomato"))
        .withColumn(
            "utilisation",
            F.when(
                F.col("on_shift_minutes") > 0,
                F.least(F.col("busy_minutes") / F.col("on_shift_minutes"), F.lit(1.0)),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "source",
            "courier_id",
            "city",
            "active_days",
            "deliveries",
            "busy_minutes",
            "on_shift_minutes",
            "utilisation",
        )
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
    parser = argparse.ArgumentParser(description="courier utilisation (Zomato only)")
    parser.add_argument("--source", default="all", choices=["all", "amazon", "zomato"])
    parser.add_argument("--master", default=None)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args(argv)

    spark = config.build_session("ilm-spark-courier-utilisation", args.master)
    try:
        fct = None
        if args.sample:
            from spark import sample

            fct = sample.sample_fct(spark)
        result = run(spark, source=args.source, fct=fct, write=not args.no_write)
        result.show(20, truncate=False)
        print(f"[courier_utilisation] rows={result.count()} wrote={not args.no_write} table={MART}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
