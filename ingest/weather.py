"""Daily historical weather per city from the Open-Meteo archive API (free, no key).

Responses are cached under DATA_DIR/raw/weather/ so re-runs make no network calls.
Cities = those in raw.city_ref with enough matched pickups; date range = order dates in the data.
"""
from __future__ import annotations

import io
import json
import time
from datetime import datetime

import requests
from psycopg2.extensions import connection

from ingest.common import Settings, connect, ensure_schema

API = "https://archive-api.open-meteo.com/v1/archive"
DAILY = "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,wind_speed_10m_max,weather_code"


def order_date_range(conn: connection) -> tuple[str, str]:
    with conn.cursor() as cur:
        cur.execute("""
            WITH d AS (SELECT to_date(order_date,'YYYY-MM-DD') d FROM raw.amazon_deliveries
                       WHERE order_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
                       UNION ALL SELECT to_date(order_date,'DD-MM-YYYY') FROM raw.zomato_deliveries
                       WHERE order_date ~ '^\\d{2}-\\d{2}-\\d{4}$')
            SELECT min(d), max(d) FROM d""")
        lo, hi = cur.fetchone()
    return lo.isoformat(), hi.isoformat()


def target_cities(conn: connection, min_pickups: int) -> list[tuple[str, float, float]]:
    with conn.cursor() as cur:
        cur.execute("""SELECT c.city, c.latitude, c.longitude FROM raw.city_ref c
            JOIN (SELECT city, count(*) n FROM raw.delivery_geo
                  WHERE matched AND point_kind='pickup' GROUP BY city) g USING (city)
            WHERE g.n >= %s ORDER BY c.city""", (min_pickups,))
        return cur.fetchall()


def fetch_city(city: str, lat: float, lon: float, start: str, end: str, cache_dir) -> dict:
    cache = cache_dir / f"{city.lower().replace(' ', '_')}_{start}_{end}.json"
    if cache.exists():
        return json.loads(cache.read_text())
    params = {"latitude": round(lat, 4), "longitude": round(lon, 4), "start_date": start,
              "end_date": end, "daily": DAILY, "timezone": "Asia/Kolkata"}
    for attempt in range(4):
        r = requests.get(API, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(15 * (attempt + 1))
            continue
        r.raise_for_status()
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache.write_text(r.text)
        return r.json()
    raise RuntimeError(f"Open-Meteo rate limited for {city}")


def main() -> None:
    settings = Settings.from_env()
    conn = connect()
    ensure_schema(conn)
    start, end = order_date_range(conn)
    cities = target_cities(conn, 20 if settings.sample_mode else 200)
    cache_dir = settings.data_dir / "raw" / "weather"
    buf = io.StringIO()
    rows = 0
    for city, lat, lon in cities:
        d = fetch_city(city, lat, lon, start, end, cache_dir)["daily"]
        for i, day in enumerate(d["time"]):
            vals = [d[k][i] for k in ("temperature_2m_max", "temperature_2m_min", "precipitation_sum",
                                      "rain_sum", "wind_speed_10m_max", "weather_code")]
            buf.write(",".join([city, day] + ["" if v is None else str(v) for v in vals]
                               + [settings.load_id]) + "\n")
            rows += 1
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.weather_daily")
        cur.copy_expert("""COPY raw.weather_daily (city, date, temp_max_c, temp_min_c, precipitation_mm,
            rain_mm, wind_max_kmh, weather_code, _load_id) FROM STDIN WITH (FORMAT csv)""", buf)
        cur.execute("UPDATE raw.ingest_runs SET rows_weather=%s WHERE load_id=%s",
                    (rows, settings.load_id))
    conn.commit()
    print(f"[weather] {len(cities)} cities, {start}..{end}, {rows} city-days "
          f"(fetched {datetime.now():%H:%M:%S})")


if __name__ == "__main__":
    main()
