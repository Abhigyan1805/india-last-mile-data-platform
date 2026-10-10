"""End-to-end test of the Spark entry point against the sample fixture."""

import pytest

from spark import run_all


def test_run_all_sample_no_write(spark):
    counts = run_all.run_all(spark, use_sample=True, write=False)
    assert counts == {
        "pincode_hour": 7,
        "courier_utilisation": 2,
        "delay_impact": 20,
        "city_comparison": 5,
    }


def test_run_all_job_subset(spark):
    counts = run_all.run_all(
        spark, use_sample=True, write=False, jobs=["pincode_hour", "city_comparison"]
    )
    assert set(counts) == {"pincode_hour", "city_comparison"}


def test_run_all_rejects_unknown_job(spark):
    with pytest.raises(ValueError):
        run_all.run_all(spark, use_sample=True, write=False, jobs=["nope"])


def test_run_all_source_filter(spark):
    counts = run_all.run_all(spark, use_sample=True, write=False, source="zomato")
    # Zomato-only: 4 pincode/hour rows, 2 couriers, 10 delay groups, 2 city rows.
    assert counts["pincode_hour"] == 4
    assert counts["courier_utilisation"] == 2
    assert counts["delay_impact"] == 10
    assert counts["city_comparison"] == 2
