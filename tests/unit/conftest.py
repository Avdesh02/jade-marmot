from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

from jade_marmot_wind.sites import load_sites  # noqa: E402

FIXED_START = datetime(2026, 11, 10, 6, 0, tzinfo=timezone.utc)


def utc(value: datetime | None) -> datetime | None:
    """PySpark hands TIMESTAMPs back as naive datetimes in the *machine's* local zone, not
    the session zone. Normalise to aware UTC so assertions hold on any developer laptop."""
    return None if value is None else value.astimezone(timezone.utc)


def pytest_collection_modifyitems(config, items):
    if not ON_DATABRICKS:
        return
    skip = pytest.mark.skip(reason="needs the local git checkout")
    for item in items:
        if "local_only" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if ON_DATABRICKS:
        yield SparkSession.builder.getOrCreate()
        return
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("jade_marmot_unit_tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sites():
    return load_sites()


@pytest.fixture
def site(sites):
    return sites["RJ01"]


def to_df(spark, records: list[dict], schema):
    """createDataFrame from dicts in schema order. Works on Spark Connect too, which has no
    RDDs (so no spark.read.json over an in-memory list)."""
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in records], schema
    )
