"""Shared fixtures for the Spark job tests.

A single local SparkSession is reused for the whole test session (Spark startup
dominates the runtime).  ``local[1]`` keeps memory and thread churn minimal on
the shared machine.
"""

import pytest

from spark import config, sample


@pytest.fixture(scope="session")
def spark():
    session = config.build_session("ilm-spark-tests", master="local[1]")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fct(spark):
    return sample.sample_fct(spark).cache()


@pytest.fixture(scope="session")
def weather(spark):
    return sample.sample_weather(spark).cache()


@pytest.fixture(scope="session")
def as_dict():
    """Return a collector: {tuple(key cols): tuple(value cols)} for tiny frames."""

    def _as_dict(df, key_cols, value_cols):
        out = {}
        for row in df.collect():
            out[tuple(row[c] for c in key_cols)] = tuple(row[c] for c in value_cols)
        return out

    return _as_dict
