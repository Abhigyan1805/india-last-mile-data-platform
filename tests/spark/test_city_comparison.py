"""Tests for the city comparison mart."""

import pytest

from spark.jobs import city_comparison


@pytest.fixture(scope="module")
def result(fct):
    return city_comparison.transform(fct)


def test_columns_and_row_count(result):
    assert result.columns == city_comparison.MART_COLUMNS
    assert result.count() == 5


def test_bengaluru_and_mumbai(result, as_dict):
    rows = as_dict(
        result,
        ["source", "city"],
        [
            "deliveries",
            "avg_delivery_min",
            "pct_over_45_min",
            "avg_distance_km",
            "pincode_match_rate",
        ],
    )
    assert rows[("amazon", "Bengaluru")] == (2, 30.0, 0.0, 4.75, 1.0)
    assert rows[("zomato", "Bengaluru")] == (3, 25.0, 0.0, 3.5, 1.0)
    # Mumbai amazon: a4 has no drop pincode -> match rate 0.5; one of two > 45 min.
    deliv, avg, over45, dist, match = rows[("amazon", "Mumbai")]
    assert (deliv, avg, over45) == (2, 45.0, 50.0)
    assert dist == pytest.approx(7.5)
    assert match == pytest.approx(0.5)
    assert rows[("zomato", "Mumbai")] == (2, 50.0, 50.0, 10.0, 1.0)
