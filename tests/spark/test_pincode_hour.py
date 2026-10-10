"""Tests for the pincode/hour demand mart."""

import pytest

from spark.jobs import pincode_hour


@pytest.fixture(scope="module")
def result(fct):
    return pincode_hour.transform(fct)


def test_row_count_and_columns(result):
    assert result.count() == 7
    assert result.columns == [
        "source",
        "pincode",
        "city",
        "hour_of_day",
        "deliveries",
        "avg_delivery_min",
        "p90_delivery_min",
    ]


def test_invalid_and_unmatched_rows_excluded(result, as_dict):
    rows = as_dict(result, ["source", "pincode", "city", "hour_of_day"], ["deliveries"])
    # a4 has a NULL drop pincode and a6 is invalid, so neither forms a group.
    assert ("amazon", "400050", "Mumbai", 18) in rows
    assert len(rows) == 7


def test_bengaluru_hour8_aggregates(result, as_dict):
    rows = as_dict(
        result,
        ["source", "pincode", "city", "hour_of_day"],
        ["deliveries", "avg_delivery_min"],
    )
    assert rows[("amazon", "560034", "Bengaluru", 8)] == (2, 30.0)
    assert rows[("zomato", "560034", "Bengaluru", 8)] == (2, 22.5)
    assert rows[("zomato", "560034", "Bengaluru", 9)] == (1, 30.0)


def test_source_filter(fct):
    only_zomato = pincode_hour.transform(fct, source="zomato")
    assert only_zomato.count() == 4
    assert {r["source"] for r in only_zomato.collect()} == {"zomato"}
