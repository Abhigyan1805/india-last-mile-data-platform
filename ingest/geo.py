"""Nearest-pincode enrichment for every pickup/drop coordinate (KD-tree on the unit sphere).

For each point we find the nearest post-office location (India Post directory, per-pincode median
coordinate to damp bad geocodes), then report distance, pincode, district and canonical city.
`matched` = inside India's bounding box AND nearest pincode within GEO_MATCH_MAX_KM.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd
from psycopg2.extensions import connection
from scipy.spatial import cKDTree

from ingest.city_map import city_for_district
from ingest.common import INDIA_BBOX, Settings, connect, ensure_schema

EARTH_KM = 6371.0088


def to_xyz(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    la, lo = np.radians(lat), np.radians(lon)
    return np.column_stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)])


def chord_to_km(chord: np.ndarray) -> np.ndarray:
    return 2 * EARTH_KM * np.arcsin(np.clip(chord / 2, 0, 1))


def in_bbox(lat: pd.Series, lon: pd.Series) -> pd.Series:
    la0, la1, lo0, lo1 = INDIA_BBOX
    return lat.between(la0, la1) & lon.between(lo0, lo1)


def query_df(conn: connection, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def load_pincode_points(conn: connection) -> pd.DataFrame:
    df = query_df(
        conn, "SELECT pincode, officename, district, statename, latitude, longitude "
        "FROM raw.pincodes WHERE upper(coalesce(district,'NA')) NOT IN ('NA','')")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df[in_bbox(df["latitude"], df["longitude"])].dropna(subset=["pincode"])
    g = df.groupby("pincode")
    ref = g.agg(latitude=("latitude", "median"), longitude=("longitude", "median"),
                office_name=("officename", "first"), district=("district", lambda s: s.mode().iat[0]),
                state=("statename", lambda s: s.mode().iat[0])).reset_index()
    return ref


def collect_points(conn: connection, settings: Settings) -> pd.DataFrame:
    parts = []
    queries = {
        "amazon": ("raw.amazon_deliveries", "order_id", "store_latitude", "store_longitude",
                   "drop_latitude", "drop_longitude"),
        "zomato": ("raw.zomato_deliveries", "id", "restaurant_latitude", "restaurant_longitude",
                   "delivery_location_latitude", "delivery_location_longitude"),
    }
    for source, (tbl, oid, plat, plon, dlat, dlon) in queries.items():
        raw = query_df(conn, f"SELECT {oid} AS order_id, {plat} p_lat, {plon} p_lon, "
                            f"{dlat} d_lat, {dlon} d_lon FROM {tbl}")
        for kind, a, b in (("pickup", "p_lat", "p_lon"), ("drop", "d_lat", "d_lon")):
            parts.append(pd.DataFrame({
                "source": source, "order_id": raw["order_id"], "point_kind": kind,
                "latitude": pd.to_numeric(raw[a], errors="coerce"),
                "longitude": pd.to_numeric(raw[b], errors="coerce")}))
    pts = pd.concat(parts, ignore_index=True)
    return pts.drop_duplicates(["source", "order_id", "point_kind"])


def enrich(points: pd.DataFrame, ref: pd.DataFrame, max_km: float) -> pd.DataFrame:
    tree = cKDTree(to_xyz(ref["latitude"].to_numpy(), ref["longitude"].to_numpy()))
    out = points.copy()
    out["in_india_bbox"] = in_bbox(out["latitude"], out["longitude"]).fillna(False)
    valid = out["in_india_bbox"].to_numpy()
    idx = np.zeros(len(out), dtype=int)
    dist = np.full(len(out), np.nan)
    if valid.any():
        d, i = tree.query(to_xyz(out.loc[valid, "latitude"].to_numpy(),
                                 out.loc[valid, "longitude"].to_numpy()), k=1)
        idx[valid], dist[valid] = i, chord_to_km(d)
    match = ref.iloc[idx].reset_index(drop=True)
    for col in ("pincode", "office_name", "district", "state"):
        out[col] = np.where(valid, match[col].to_numpy(), None)
    out["distance_km"] = dist
    out["matched"] = valid & (np.nan_to_num(dist, nan=1e9) <= max_km)
    # unmatched points keep their nearest pincode for diagnostics but get no city
    out["city"] = [city_for_district(d) if m else None for d, m in zip(out["district"], out["matched"], strict=True)]
    return out


def write_geo(conn: connection, geo: pd.DataFrame, load_id: str) -> None:
    cols = ["source", "order_id", "point_kind", "latitude", "longitude", "in_india_bbox", "pincode",
            "office_name", "district", "state", "city", "distance_km", "matched"]
    buf = io.StringIO()
    geo.assign(_load_id=load_id)[cols + ["_load_id"]].to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.delivery_geo")
        cur.copy_expert(
            f"COPY raw.delivery_geo ({', '.join(cols)}, _load_id) FROM STDIN WITH (FORMAT csv)", buf)
        # city reference = centroid of pickup points per city (used for weather lookups)
        cur.execute("TRUNCATE raw.city_ref")
        cur.execute("""INSERT INTO raw.city_ref (city, state, latitude, longitude)
            SELECT city, mode() WITHIN GROUP (ORDER BY state), avg(latitude), avg(longitude)
            FROM raw.delivery_geo WHERE matched AND point_kind='pickup' AND city IS NOT NULL
            GROUP BY city""")


def main() -> None:
    settings = Settings.from_env()
    conn = connect()
    ensure_schema(conn)
    ref = load_pincode_points(conn)
    geo = enrich(collect_points(conn, settings), ref, settings.geo_match_max_km)
    write_geo(conn, geo, settings.load_id)
    n, m = len(geo), int(geo["matched"].sum())
    rate = m / n if n else 0.0
    by_src = geo.groupby("source")["matched"].mean().round(4).to_dict()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw.ingest_runs (load_id, geo_points, geo_matched, geo_match_rate)
               VALUES (%s,%s,%s,%s) ON CONFLICT (load_id) DO UPDATE SET
               geo_points=EXCLUDED.geo_points, geo_matched=EXCLUDED.geo_matched,
               geo_match_rate=EXCLUDED.geo_match_rate""", (settings.load_id, n, m, rate))
    conn.commit()
    print(f"[geo] {n} points, {m} matched within {settings.geo_match_max_km} km "
          f"({rate:.2%}); by source {by_src}; {len(ref)} reference pincodes")


if __name__ == "__main__":
    main()
