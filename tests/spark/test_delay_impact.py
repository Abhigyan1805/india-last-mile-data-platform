"""Tests for the delay-impact mart."""

import pytest

from spark.jobs import delay_impact


@pytest.fixture(scope="module")
def result(fct, weather):
    return delay_impact.transform(fct, dim_weather=weather)


def test_columns_and_factors(result):
    assert result.columns == delay_impact.MART_COLUMNS
    factors = {r["factor"] for r in result.collect()}
    assert factors == {"weather", "traffic", "festival", "rain_bucket"}


def test_baseline_is_zero_for_lowest_impact_value(result, as_dict):
    rows = as_dict(
        result, ["source", "factor", "factor_value"], ["avg_delivery_min", "delay_vs_baseline_min"]
    )
    # Amazon weather: Fog (20) is the baseline; Sunny (25) is +5.
    assert rows[("amazon", "weather", "Fog")][1] == pytest.approx(0.0)
    assert rows[("amazon", "weather", "Sunny")][1] == pytest.approx(5.0)
    assert rows[("amazon", "weather", "Cloudy")][1] == pytest.approx(25.0)


def test_festival_is_zomato_only(result, as_dict):
    rows = as_dict(
        result,
        ["source", "factor", "factor_value"],
        ["deliveries", "avg_delivery_min", "delay_vs_baseline_min"],
    )
    assert ("amazon", "festival", "true") not in rows
    assert ("amazon", "festival", "false") not in rows
    # Festival day (True) is slower than a normal day (False baseline 25).
    assert rows[("zomato", "festival", "false")] == (3, 25.0, 0.0)
    assert rows[("zomato", "festival", "true")] == (2, 50.0, 25.0)


def test_rain_bucket_joined_from_dimension(result, as_dict):
    rows = as_dict(
        result,
        ["source", "factor", "factor_value"],
        ["deliveries", "avg_delivery_min", "delay_vs_baseline_min"],
    )
    # Amazon: moderate (20) baseline, heavy (45) is +25.
    assert rows[("amazon", "rain_bucket", "moderate")][2] == pytest.approx(0.0)
    assert rows[("amazon", "rain_bucket", "heavy")][2] == pytest.approx(25.0)
    # Zomato: none (25) baseline, heavy (50) is +25.
    assert rows[("zomato", "rain_bucket", "none")][2] == pytest.approx(0.0)
    assert rows[("zomato", "rain_bucket", "heavy")][2] == pytest.approx(25.0)


def test_without_dimension_rain_factor_absent(fct):
    result = delay_impact.transform(fct, dim_weather=None)
    assert "rain_bucket" not in {r["factor"] for r in result.collect()}


def test_confidence_intervals_shape(fct, weather):
    ci = delay_impact.confidence_intervals(fct, dim_weather=weather)
    assert set(ci.columns) == {
        "source",
        "factor",
        "factor_value",
        "deliveries",
        "avg_delivery_min",
        "stddev_min",
        "ci95_half_width",
    }
    rows = {r["factor"] for r in ci.collect()}
    assert rows == {"weather", "traffic", "festival", "rain_bucket"}
