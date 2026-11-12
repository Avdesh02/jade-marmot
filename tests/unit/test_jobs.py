import json
from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from conftest import FIXED_START
from jade_marmot_wind.jobs import plan_simulation, simulate_site
from jade_marmot_wind.jobs.alert_digest import build_digest


def test_plan_simulation_resolves_sites_and_prepares_folders(tmp_path, capsys):
    args = plan_simulation.parse_args(
        [
            "--enabled-sites=RJ01,TN01,GJ01",
            "--site-ids=gj01,rj01",
            f"--landing-root={tmp_path}",
        ]
    )
    inputs = plan_simulation.run(args)
    assert inputs == [
        {"site_id": "GJ01", "turbine_count": 4},
        {"site_id": "RJ01", "turbine_count": 4},
    ]
    # Every enabled site gets landing folders, not just the ones simulated this run.
    for site in ("RJ01", "TN01", "GJ01"):
        assert (tmp_path / "sensors" / site).is_dir()
        assert (tmp_path / "turbine_cdc" / site).is_dir()
    assert not (tmp_path / "sensors" / "KA01").exists()
    assert 'task value site_ids = "GJ01,RJ01"' in capsys.readouterr().out


def test_plan_simulation_rejects_site_outside_target(tmp_path):
    args = plan_simulation.parse_args(
        ["--enabled-sites=RJ01", "--site-ids=KA01", f"--landing-root={tmp_path}"]
    )
    with pytest.raises(ValueError, match="not enabled"):
        plan_simulation.run(args)


def _simulate(tmp_path, *extra):
    return simulate_site.run(
        simulate_site.parse_args(
            [
                "--site-id=RJ01",
                "--enabled-sites=all",
                f"--landing-root={tmp_path}",
                "--start-ts=2026-11-10T06:00:00Z",
                "--minutes=30",
                *extra,
            ]
        )
    )


def test_simulate_site_seeds_master_data_once(tmp_path):
    first = _simulate(tmp_path, "--own-run-id=111")
    assert first["run_id"] == "111"
    assert first["seeded_master_data"] and first["cdc_events"] == 4
    second = _simulate(tmp_path, "--own-run-id=222")
    assert not second["seeded_master_data"] and second["cdc_events"] == 0
    third = _simulate(tmp_path, "--own-run-id=333", "--reseed-master-data=true")
    assert third["seeded_master_data"]


def test_simulate_site_writes_manifest_under_parent_run_id(tmp_path):
    m = _simulate(tmp_path, "--run-id=999", "--own-run-id=111", "--fault-rate=0.2")
    path = tmp_path / "_manifests" / "999" / "RJ01.json"
    assert json.loads(path.read_text()) == m
    assert m["readings_written"] == 4 * 30 + m["faults"].get("duplicate", 0)
    assert m["expected_clean_readings"] + m["expected_quarantined"] <= m["readings_written"]


def test_build_digest(spark):
    t0 = FIXED_START
    alerts = spark.createDataFrame(
        [
            ("RJ01", "RJ01-WTG01", t0, "gearbox_oil_temp_c", "critical"),
            ("RJ01", "RJ01-WTG02", t0 + timedelta(hours=1), "vibration_mm_s", "warning"),
            ("RJ01", "RJ01-WTG02", t0 - timedelta(hours=48), "vibration_mm_s", "warning"),
            ("TN01", "TN01-WTG01", t0, "wind_speed_mps", "critical"),
        ],
        "site_id string, turbine_id string, event_ts timestamp, metric string, severity string",
    )
    rows = {r["site_id"]: r for r in build_digest(alerts, "77", ["RJ01"], 24).collect()}
    assert list(rows) == ["RJ01"]
    rj = rows["RJ01"]
    assert rj["run_id"] == "77"
    assert (rj["alert_count"], rj["critical_count"], rj["warning_count"]) == (2, 1, 1)
    assert rj["turbines_affected"] == 2
    assert rj["metrics"] == ["gearbox_oil_temp_c", "vibration_mm_s"]

    everything = build_digest(alerts, "77", None, 24)
    assert everything.select(F.sum("alert_count")).first()[0] == 3
