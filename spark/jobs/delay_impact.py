"""Mart 3 -- impact of weather, traffic, festival days and rain on delivery time.

Question: *what makes deliveries slower?*  Each factor (``weather``, ``traffic``,
``festival``, ``rain_bucket``) is exploded into (factor, factor_value) rows and
grouped, then compared against the *lowest-impact* value of that factor -- the
value with the smallest mean delivery time.  ``delay_vs_baseline_min`` is the
extra minutes versus that baseline, so the baseline row is 0 and every other
value is positive.

Rain comes from ``analytics.dim_weather_daily.rain_bucket`` (none/light/
moderate/heavy) joined on city + date; it is optional so the transform can be
unit-tested without the dimension.  The stored mart keeps exactly the DDL
columns; the CLI also prints a 95% confidence interval per group (mean +/- 1.96
* sd / sqrt(n)) so the numbers can be quoted with a margin of error.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark import config, io

MART = config.MART_TABLES["delay_impact"]
MART_COLUMNS = [
    "source",
    "factor",
    "factor_value",
    "deliveries",
    "avg_delivery_min",
    "delay_vs_baseline_min",
]


def _factor_frame(fct: DataFrame, factor_name: str, value_col: str) -> DataFrame:
    return fct.select(
        F.col("source"),
        F.lit(factor_name).alias("factor"),
        F.col(value_col).cast("string").alias("factor_value"),
        F.col("delivery_minutes"),
    ).filter(F.col("factor_value").isNotNull())


def _factor_long(fct: DataFrame, dim_weather: DataFrame | None = None) -> DataFrame:
    """Explode every factor into one long (source, factor, factor_value, minutes) frame."""
    df = fct.filter(F.col("is_valid")) if "is_valid" in fct.columns else fct
    frames = [
        _factor_frame(df, "weather", "weather"),
        _factor_frame(df, "traffic", "traffic"),
        _factor_frame(df, "festival", "festival"),
    ]
    if dim_weather is not None:
        rain = df.join(
            F.broadcast(dim_weather.select("city", "date", "rain_bucket")),
            on=[df["city"] == dim_weather["city"], df["order_date"] == dim_weather["date"]],
            how="left",
        )
        frames.append(_factor_frame(rain, "rain_bucket", "rain_bucket"))
    long = frames[0]
    for extra in frames[1:]:
        long = long.unionByName(extra)
    return long


def transform(
    fct: DataFrame, source: str | None = None, dim_weather: DataFrame | None = None
) -> DataFrame:
    """Build the delay-impact mart with delay relative to each factor's baseline."""
    long = _factor_long(fct, dim_weather)
    if source and source != "all":
        long = long.filter(F.col("source") == source)

    grouped = long.groupBy("source", "factor", "factor_value").agg(
        F.count("*").cast("long").alias("deliveries"),
        F.avg("delivery_minutes").alias("avg_delivery_min"),
    )
    window = Window.partitionBy("source", "factor")
    return (
        grouped.withColumn("baseline", F.min("avg_delivery_min").over(window))
        .withColumn("delay_vs_baseline_min", F.col("avg_delivery_min") - F.col("baseline"))
        .select(*MART_COLUMNS)
    )


def confidence_intervals(
    fct: DataFrame, source: str | None = None, dim_weather: DataFrame | None = None
) -> DataFrame:
    """Return per-group mean, stddev, n and the 95% CI half-width (for reporting)."""
    long = _factor_long(fct, dim_weather)
    if source and source != "all":
        long = long.filter(F.col("source") == source)
    return long.groupBy("source", "factor", "factor_value").agg(
        F.count("*").cast("long").alias("deliveries"),
        F.avg("delivery_minutes").alias("avg_delivery_min"),
        F.stddev_samp("delivery_minutes").alias("stddev_min"),
        (
            F.lit(1.96) * F.stddev_samp("delivery_minutes") / F.sqrt(F.count("*").cast("double"))
        ).alias("ci95_half_width"),
    )


def run(
    spark,
    *,
    source: str | None = None,
    fct: DataFrame | None = None,
    dim_weather: DataFrame | None = None,
    write: bool = True,
    report: bool = False,
) -> DataFrame:
    if fct is None:
        fct = io.read_fct(spark)
    if dim_weather is None:
        dim_weather = io.read_dim_weather(spark)
    result = transform(fct, source=source, dim_weather=dim_weather)
    if write:
        io.write_mart(result, MART)
    if report:
        confidence_intervals(fct, source=source, dim_weather=dim_weather).orderBy(
            "source", "factor", "avg_delivery_min"
        ).show(50, truncate=False)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="delay impact of weather/traffic/festival/rain")
    parser.add_argument("--source", default="all", choices=["all", "amazon", "zomato"])
    parser.add_argument("--master", default=None)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--report", action="store_true", help="print 95% confidence intervals")
    args = parser.parse_args(argv)

    spark = config.build_session("ilm-spark-delay-impact", args.master)
    try:
        fct = dim = None
        if args.sample:
            from spark import sample

            fct, dim = sample.sample_fct(spark), sample.sample_weather(spark)
        result = run(
            spark,
            source=args.source,
            fct=fct,
            dim_weather=dim,
            write=not args.no_write,
            report=args.report,
        )
        result.orderBy("source", "factor", "avg_delivery_min").show(50, truncate=False)
        print(f"[delay_impact] rows={result.count()} wrote={not args.no_write} table={MART}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
