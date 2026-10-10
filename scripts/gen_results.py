"""Generate docs/RESULTS.md from the live warehouse after an end-to-end run (numbers are queried, never typed)."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from ingest.common import ROOT, connect

COUNT_SQL = (
    "SELECT table_schema||'.'||table_name, (xpath('/row/c/text()', query_to_xml("
    "'select count(*) c from '||table_schema||'.'||table_name,false,true,'')))[1]::text "
    "FROM information_schema.tables WHERE table_schema IN ('analytics','spark_marts') "
    "AND table_type='BASE TABLE' ORDER BY 1"
)


def q(cur, sql: str) -> list[tuple]:
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as exc:  # a component's table may be absent
        cur.connection.rollback()
        return [(f"unavailable: {type(exc).__name__}",)]


def table(rows: list[tuple], header: list[str]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


SECTIONS = [
    ("Ingest", """SELECT load_id, sample_mode, rows_amazon, rows_zomato, rows_pincodes, rows_weather, geo_points,
        geo_matched, round((geo_match_rate*100)::numeric,2) FROM raw.ingest_runs
        WHERE geo_points IS NOT NULL ORDER BY started_at DESC NULLS LAST LIMIT 1""",
     ["load_id", "sample", "amazon rows", "zomato rows", "pincode rows", "weather city-days", "geo points",
      "matched", "match rate %"]),
    ("Pincode match by source / point", """SELECT source, point_kind, count(*), count(*) FILTER (WHERE matched),
        round((100.0*avg(matched::int))::numeric,2), round(avg(distance_km)::numeric,2)
        FROM raw.delivery_geo GROUP BY 1,2 ORDER BY 1,2""",
     ["source", "point", "points", "matched", "match %", "avg km to pincode"]),
    ("Data-quality checks (failing rows caught)", """SELECT layer, check_name, dataset, rows_checked, rows_failed
        FROM quality.check_results WHERE run_id IN (SELECT run_id FROM quality.check_results
        GROUP BY run_id ORDER BY max(run_ts) DESC LIMIT 2) ORDER BY layer, check_name, dataset""",
     ["layer", "check", "dataset", "rows checked", "rows failed"]),
    ("Warehouse row counts", COUNT_SQL, ["table", "rows"]),
    ("Streaming (SIMULATED events)", """SELECT load_id, produced, consumed, landed, duplicates_dropped, late_events,
        out_of_bounds, malformed, round(duration_s::numeric,1), round(events_per_s::numeric)
        FROM raw.stream_runs ORDER BY produced DESC NULLS LAST LIMIT 1""",
     ["load_id", "produced", "consumed", "landed", "duplicates dropped", "late", "out of bounds", "malformed",
      "seconds", "events/s"]),
    ("Delay impact (Zomato)", """SELECT factor, factor_value, deliveries, round(avg_delivery_min::numeric,1),
        round(delay_vs_baseline_min::numeric,1) FROM spark_marts.delay_impact WHERE source='zomato'
        ORDER BY 1, 5 DESC""", ["factor", "value", "deliveries", "avg min", "delay vs baseline min"]),
    ("City comparison (top 12 by volume)", """SELECT source, city, deliveries, round(avg_delivery_min::numeric,1),
        round(pct_over_45_min::numeric,1) FROM spark_marts.city_comparison ORDER BY deliveries DESC LIMIT 12""",
     ["source", "city", "deliveries", "avg min", "% over 45 min"]),
    ("Busiest pincode-hours", """SELECT source, pincode, city, hour_of_day, deliveries
        FROM spark_marts.deliveries_per_pincode_hour ORDER BY deliveries DESC LIMIT 10""",
     ["source", "pincode", "city", "hour", "deliveries"]),
    ("Courier utilisation (distribution)", """SELECT count(*), round(avg(utilisation)::numeric,3),
        round(percentile_cont(0.5) WITHIN GROUP (ORDER BY utilisation)::numeric,3),
        round(max(utilisation)::numeric,3) FROM spark_marts.courier_utilisation""",
     ["courier-city rows", "mean", "median", "max"]),
]


def main() -> None:
    data = Path(os.environ.get("DATA_DIR", ROOT / "data"))
    cur = connect().cursor()
    md = [f"# Results of the recorded end-to-end run\n\nGenerated {datetime.now():%Y-%m-%d %H:%M} by "
          "`scripts/e2e_run.sh` (native runtime, no Docker). Every number is queried from the warehouse.\n"]
    for title, sql, hdr in SECTIONS:
        md.append(f"\n## {title}\n" + table(q(cur, sql), hdr))
    timings = [ln.split("\t") for ln in (data / "e2e_timings.tsv").read_text().splitlines() if ln]
    total = (data / "e2e_total_s").read_text().strip()
    md.append("\n## Runtime (seconds per step)\n" + table(timings, ["step", "seconds"])
              + f"\n\n**Total wall clock: {total} s**\n")
    (ROOT / "docs" / "RESULTS.md").write_text("\n".join(md))
    print("wrote docs/RESULTS.md")


if __name__ == "__main__":
    main()
