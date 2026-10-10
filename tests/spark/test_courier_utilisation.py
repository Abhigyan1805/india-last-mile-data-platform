"""Tests for the courier utilisation mart (Zomato only)."""

import pytest

from spark.jobs import courier_utilisation


@pytest.fixture(scope="module")
def result(fct):
    return courier_utilisation.transform(fct)


def test_only_zomato_and_columns(result):
    assert result.count() == 2
    assert {r["source"] for r in result.collect()} == {"zomato"}
    assert result.columns == [
        "source",
        "courier_id",
        "city",
        "active_days",
        "deliveries",
        "busy_minutes",
        "on_shift_minutes",
        "utilisation",
    ]


def test_utilisation_maths(result, as_dict):
    rows = as_dict(
        result,
        ["courier_id"],
        ["active_days", "deliveries", "busy_minutes", "on_shift_minutes", "utilisation"],
    )
    # DEHRES001: 08:00 -> 09:30 = 90 min on shift, 75 min busy.
    days, deliv, busy, shift, util = rows[("DEHRES001",)]
    assert (days, deliv, busy) == (1, 3, 75.0)
    assert shift == pytest.approx(90.0)
    assert util == pytest.approx(75.0 / 90.0)

    # DEHRES002: 18:00 -> 19:40 = 100 min on shift, 100 min busy -> capped at 1.0.
    days, deliv, busy, shift, util = rows[("DEHRES002",)]
    assert (days, deliv, busy) == (1, 2, 100.0)
    assert shift == pytest.approx(100.0)
    assert util == pytest.approx(1.0)


def test_dominant_city(result, as_dict):
    rows = as_dict(result, ["courier_id"], ["city"])
    assert rows[("DEHRES001",)] == ("Bengaluru",)
    assert rows[("DEHRES002",)] == ("Mumbai",)
