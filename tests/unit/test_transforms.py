from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from conftest import FIXED_START, to_df, utc
from jade_marmot_wind.simulator import simulate_site
from jade_marmot_wind.transforms import (
    DEFAULT_THRESHOLDS,
    READING_RULES,
    SENSOR_PAYLOAD_SCHEMA,
    TURBINE_CDC_SCHEMA,
    Threshold,
    dim_site,
    failing_rows,
    fleet_status,
    hourly_metrics,
    parse_sensor_readings,
    passing_rows,
    silver_readings,
    site_daily_kpis,
    standardise_turbine_cdc,
    thresholds_from_conf,
    turbine_alerts,
    with_rule_results,
)

T0 = FIXED_START
EPOCH = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _bronze(spark, records):
    df = to_df(spark, records, SENSOR_PAYLOAD_SCHEMA)
    return df.select(
        "*",
        F.lit(None).cast("string").alias("_rescued_data"),
        F.lit("file:/test.json").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _checked(spark, records):
    return with_rule_results(parse_sensor_readings(_bronze(spark, records)), READING_RULES)


@pytest.fixture(scope="module")
def sim(sites):
    return simulate_site(
        sites["RJ01"], start=FIXED_START, minutes=240, fault_rate=0.15, seed="transforms"
    )


@pytest.fixture(scope="module")
def checked(spark, sim):
    # No .cache(): serverless compute doesn't support it (NOT_SUPPORTED_WITH_SERVERLESS).
    return _checked(spark, sim.readings)


# --- Quality rules ----------------------------------------------------------------------


def test_every_injected_fault_is_quarantined(checked, sim):
    quarantined = sum(n for kind, n in sim.faults.items() if kind != "duplicate")
    assert quarantined > 0
    assert failing_rows(checked).count() == quarantined
    assert passing_rows(checked).count() == len(sim.readings) - quarantined


def test_failed_rules_name_the_problem(spark, sim):
    good = sim.readings[0]
    bad = [
        {**good, "reading_id": "a", "wind_speed_mps": None},
        {**good, "reading_id": "b", "event_ts": "garbage"},
        {**good, "reading_id": "c", "site_id": "ZZ99"},
        {**good, "reading_id": "d", "power_output_kw": -750.0, "gearbox_oil_temp_c": 999.0},
    ]
    got = {
        r["reading_id"]: sorted(r["_failed_rules"])
        for r in failing_rows(_checked(spark, bad)).collect()
    }
    assert got == {
        "a": ["wind_speed_in_range"],
        "b": ["valid_event_ts"],
        "c": ["site_matches_turbine"],
        "d": ["gearbox_temp_plausible", "power_in_range"],
    }


def test_rules_are_null_safe(spark, sim):
    rec = {k: None for k in sim.readings[0]}
    rec["reading_id"] = "all-null"
    (row,) = _checked(spark, [rec]).collect()
    assert set(row["_failed_rules"]) == set(READING_RULES)


def test_silver_shape(checked):
    silver = silver_readings(checked)
    assert "_failed_rules" not in silver.columns
    assert "event_ts_raw" not in silver.columns
    assert dict(silver.dtypes)["event_ts"] == "timestamp"
    assert dict(silver.dtypes)["event_date"] == "date"


# --- CDC --------------------------------------------------------------------------------


def test_standardise_turbine_cdc(spark):
    raw = [
        {
            "turbine_id": " rj01-wtg01 ",
            "site_id": "rj01",
            "model": "m",
            "capacity_mw": 2.1,
            "hub_height_m": 100.0,
            "rotor_diameter_m": 80.0,
            "commission_date": "2022-03-15",
            "status": "ACTIVE",
            "operation": "upsert",
            "change_seq": 1,
            "changed_at": "2026-11-10T06:00:00Z",
        }
    ]
    df = to_df(spark, raw, TURBINE_CDC_SCHEMA).select(
        "*",
        F.lit(None).cast("string").alias("_rescued_data"),
        F.lit("f").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    (row,) = standardise_turbine_cdc(df).collect()
    assert row["turbine_id"] == "RJ01-WTG01"
    assert row["site_id"] == "RJ01"
    assert row["operation"] == "UPSERT"
    assert row["status"] == "active"
    assert utc(row["changed_at"]) == T0
    assert str(row["commission_date"]) == "2022-03-15"


# --- Gold -------------------------------------------------------------------------------


def _reading(rid, ts, **values):
    base = {
        "reading_id": rid,
        "site_id": "RJ01",
        "turbine_id": "RJ01-WTG01",
        "event_ts": ts,
        "sample_interval_s": 60,
        "wind_speed_mps": 10.0,
        "power_output_kw": 1000.0,
        "rotor_rpm": 15.0,
        "gearbox_oil_temp_c": 60.0,
        "generator_temp_c": 80.0,
        "vibration_mm_s": 2.0,
        "status_code": "RUNNING",
    }
    base.update(values)
    return base


@pytest.fixture
def readings_df(spark):
    rows = [
        _reading("r1", T0),
        _reading("r2", T0 + timedelta(minutes=1), gearbox_oil_temp_c=85.0),
        _reading("r3", T0 + timedelta(minutes=2), gearbox_oil_temp_c=97.0, vibration_mm_s=12.0),
        _reading("r4", T0 + timedelta(hours=1), power_output_kw=0.0, status_code="MAINTENANCE"),
    ]
    return spark.createDataFrame(rows).withColumn("event_date", F.to_date("event_ts"))


def test_turbine_alerts(readings_df):
    alerts = {
        (r["reading_id"], r["metric"]): (r["severity"], r["threshold"])
        for r in turbine_alerts(readings_df, DEFAULT_THRESHOLDS).collect()
    }
    assert alerts == {
        ("r2", "gearbox_oil_temp_c"): ("warning", 80.0),
        ("r3", "gearbox_oil_temp_c"): ("critical", 95.0),
        ("r3", "vibration_mm_s"): ("critical", 11.0),
    }


def test_thresholds_from_conf():
    conf = {"wind.threshold.gearbox_oil_temp_c.warning": "70"}
    t = {x.metric: x for x in thresholds_from_conf(lambda k, d: conf.get(k, d))}
    assert t["gearbox_oil_temp_c"] == Threshold("gearbox_oil_temp_c", 70.0, 95.0)
    with pytest.raises(ValueError, match="above critical"):
        thresholds_from_conf(
            lambda k, d: "200" if k == "wind.threshold.vibration_mm_s.warning" else d
        )


def _dim(spark, versions):
    return spark.createDataFrame(
        versions,
        "turbine_id string, site_id string, model string, capacity_mw double, status string, "
        "__START_AT timestamp, __END_AT timestamp",
    )


def test_hourly_metrics_use_capacity_in_force(spark, readings_df):
    # Uprated from 2.0 to 4.0 MW at T0 + 1h: the first hour must be rated at 2.0.
    dim = _dim(
        spark,
        [
            ("RJ01-WTG01", "RJ01", "m", 2.0, "active", EPOCH, T0 + timedelta(hours=1)),
            ("RJ01-WTG01", "RJ01", "m-U", 4.0, "active", T0 + timedelta(hours=1), None),
        ],
    )
    rows = {utc(r["hour_start"]): r for r in hourly_metrics(readings_df, dim).collect()}
    h0, h1 = rows[T0], rows[T0 + timedelta(hours=1)]

    assert h0["reading_count"] == 3 and h0["capacity_mw"] == 2.0
    assert h0["energy_kwh"] == pytest.approx(50.0)  # 3 x 1000 kW x 1 min
    assert h0["capacity_factor"] == pytest.approx(50.0 / (2000 * 3 / 60))
    assert h0["availability"] == 1.0
    assert h1["capacity_mw"] == 4.0 and h1["model"] == "m-U"
    assert h1["availability"] == 0.0


def test_hourly_metrics_without_dimension_row(spark, readings_df):
    rows = hourly_metrics(readings_df, _dim(spark, [])).collect()
    assert len(rows) == 2
    assert all(r["capacity_mw"] is None and r["capacity_factor"] is None for r in rows)


def test_site_daily_kpis(spark, readings_df, sites):
    dim = _dim(spark, [("RJ01-WTG01", "RJ01", "m", 2.0, "active", EPOCH, None)])
    hourly = hourly_metrics(readings_df, dim)
    alerts = turbine_alerts(readings_df, DEFAULT_THRESHOLDS)
    sites_df = dim_site(spark, sites.values(), ["RJ01"])
    (row,) = site_daily_kpis(hourly, alerts, sites_df).collect()
    assert row["site_name"] == "Jaisalmer Wind Park"
    assert row["reading_count"] == 4
    assert row["energy_mwh"] == pytest.approx(0.05)
    assert row["critical_alerts"] == 2 and row["warning_alerts"] == 1
    assert row["availability"] == pytest.approx(0.5)


def test_fleet_status(spark, readings_df):
    current = spark.createDataFrame(
        [
            ("RJ01-WTG01", "RJ01", "m", 2.0, "active"),
            ("RJ01-WTG02", "RJ01", "m", 2.0, "active"),
            ("RJ01-WTG03", "RJ01", "m", 2.0, "maintenance"),
        ],
        "turbine_id string, site_id string, model string, capacity_mw double, status string",
    )
    alerts = turbine_alerts(readings_df, DEFAULT_THRESHOLDS)
    got = {r["turbine_id"]: r for r in fleet_status(current, readings_df, alerts).collect()}
    assert got["RJ01-WTG01"]["health_state"] == "alarm"
    assert got["RJ01-WTG01"]["last_status_code"] == "MAINTENANCE"
    assert got["RJ01-WTG01"]["critical_alerts_24h"] == 2
    assert got["RJ01-WTG02"]["health_state"] == "no_data"
    assert got["RJ01-WTG03"]["health_state"] == "maintenance"


def test_dim_site_marks_enabled(spark, sites):
    rows = {r["site_id"]: r for r in dim_site(spark, sites.values(), ["RJ01", "GJ01"]).collect()}
    assert len(rows) == 5
    assert rows["RJ01"]["is_enabled"] and not rows["KA01"]["is_enabled"]
    assert rows["GJ01"]["capacity_mw"] == pytest.approx(11.4)
