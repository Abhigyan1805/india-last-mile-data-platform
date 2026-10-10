"""Entry point for the Spark layer: ``python -m spark.run_all``.

Reads ``analytics.fct_deliveries`` (valid rows) once and feeds all four jobs,
then writes the four ``spark_marts.*`` tables.  Use ``--sample`` to run the whole
thing against the checked-in in-memory fixture with no Postgres, and
``--no-write`` to compute without persisting.  ``--jobs`` selects a subset.

Design notes
------------
* The fact table is read once and cached, so the four jobs share one JDBC pull
  instead of four.  It is unpersisted when the run finishes.
* The weather dimension is small (city x day), so it is broadcast into the
  delay-impact job rather than shuffled.
* Every job keeps the exact column set from ``ingest/sql/ddl.sql``.
"""

from __future__ import annotations

import argparse
import time

from spark import config, io
from spark.jobs import city_comparison, courier_utilisation, delay_impact, pincode_hour

JOB_NAMES = ["pincode_hour", "courier_utilisation", "delay_impact", "city_comparison"]


def run_all(
    spark,
    *,
    source: str | None = None,
    write: bool = True,
    use_sample: bool = False,
    jobs: list[str] | None = None,
    report: bool = False,
) -> dict[str, int]:
    """Run the selected jobs and return {job: row_count}."""
    selected = jobs or JOB_NAMES
    unknown = set(selected) - set(JOB_NAMES)
    if unknown:
        raise ValueError(f"unknown jobs: {sorted(unknown)}")

    if use_sample:
        from spark import sample

        fct = sample.sample_fct(spark)
        weather = sample.sample_weather(spark)
    else:
        fct = io.read_fct(spark)
        weather = io.read_dim_weather(spark)

    fct.cache()
    fct.count()  # materialise once; every job then reads from memory

    counts: dict[str, int] = {}
    try:
        if "pincode_hour" in selected:
            counts["pincode_hour"] = pincode_hour.run(
                spark, source=source, fct=fct, write=write
            ).count()
        if "courier_utilisation" in selected:
            counts["courier_utilisation"] = courier_utilisation.run(
                spark, source=source, fct=fct, write=write
            ).count()
        if "delay_impact" in selected:
            counts["delay_impact"] = delay_impact.run(
                spark, source=source, fct=fct, dim_weather=weather, write=write, report=report
            ).count()
        if "city_comparison" in selected:
            counts["city_comparison"] = city_comparison.run(
                spark, source=source, fct=fct, write=write
            ).count()
    finally:
        fct.unpersist()
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="run the PySpark mart jobs")
    parser.add_argument("--source", default="all", choices=["all", "amazon", "zomato"])
    parser.add_argument(
        "--master", default=None, help="Spark master (default $SPARK_MASTER or local[*])"
    )
    parser.add_argument(
        "--jobs", default=None, help="comma-separated subset of " + ",".join(JOB_NAMES)
    )
    parser.add_argument("--sample", action="store_true", help="use the in-memory sample fixture")
    parser.add_argument("--no-write", action="store_true", help="compute only, do not write marts")
    parser.add_argument("--report", action="store_true", help="print 95% CIs for delay impact")
    args = parser.parse_args(argv)

    jobs = args.jobs.split(",") if args.jobs else None
    spark = config.build_session("ilm-spark-run-all", args.master)
    started = time.time()
    try:
        counts = run_all(
            spark,
            source=args.source,
            write=not args.no_write,
            use_sample=args.sample,
            jobs=jobs,
            report=args.report,
        )
        print("=" * 60)
        for name, n in counts.items():
            target = config.MART_TABLES[name]
            print(f"{name:22s} rows={n:<8d} -> {target}")
        print(f"total {sum(counts.values())} rows in {time.time() - started:.1f}s")
        if not args.no_write:
            print("marts written (overwrite + truncate)")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
