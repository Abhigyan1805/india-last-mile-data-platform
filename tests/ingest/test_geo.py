import numpy as np
import pandas as pd

from ingest.city_map import city_for_district
from ingest.geo import enrich


def _ref() -> pd.DataFrame:
    return pd.DataFrame({
        "pincode": ["560034", "400001"], "office_name": ["Koramangala", "Fort"],
        "district": ["BENGALURU URBAN", "MUMBAI"], "state": ["KARNATAKA", "MAHARASHTRA"],
        "latitude": [12.93, 18.94], "longitude": [77.62, 72.83]})


def test_enrich_matches_nearest_and_flags_outliers() -> None:
    pts = pd.DataFrame({
        "source": "amazon", "order_id": ["a", "b", "c", "d"], "point_kind": "pickup",
        "latitude": [12.931, 18.95, 0.0, 28.6], "longitude": [77.621, 72.84, 0.0, 77.2]})
    out = enrich(pts, _ref(), max_km=10)
    assert out["pincode"].tolist()[:2] == ["560034", "400001"]
    assert out["matched"].tolist() == [True, True, False, False]  # (0,0) outside India; Delhi >10km
    assert out["city"].tolist()[:2] == ["Bengaluru", "Mumbai"]
    assert np.isnan(out.loc[2, "distance_km"])


def test_city_aliases() -> None:
    assert city_for_district("MUMBAI SUBURBAN") == "Mumbai"
    assert city_for_district("Some Place") == "Some Place"
    assert city_for_district(None) is None
