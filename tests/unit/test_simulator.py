import json
from datetime import datetime, timedelta, timezone

import pytest

from conftest import FIXED_START
from jade_marmot_wind.simulator import (
    CUT_IN_MPS,
    CUT_OUT_MPS,
    RATED_MPS,
    derive_seed,
    master_data_seeded,
    parse_ts,
    power_kw,
    resolve_start,
    simulate_site,
    write_landing_files,
)


def _sim(site, **kw):
    kw.setdefault("start", FIXED_START)
    kw.setdefault("minutes", 30)
    return simulate_site(site, **kw)


def test_power_curve(site):
    t = site.turbines[0]
    rated = t.capacity_mw * 1000
    assert power_kw(t, CUT_IN_MPS - 0.1) == 0
    assert 0 < power_kw(t, 8.0) < rated
    assert power_kw(t, RATED_MPS) == rated
    assert power_kw(t, 20.0) == rated
    assert power_kw(t, CUT_OUT_MPS) == 0


def test_power_curve_is_monotonic_below_rated(site):
    t = site.turbines[0]
    values = [power_kw(t, CUT_IN_MPS + i * 0.5) for i in range(int((RATED_MPS - CUT_IN_MPS) / 0.5))]
    assert values == sorted(values)


def test_volume_and_shape(site):
    r = _sim(site, minutes=30, interval_seconds=60, tag_count=5)
    assert len(r.readings) == len(site.turbines) * 30
    rec = r.readings[0]
    assert rec["site_id"] == "RJ01"
    assert rec["turbine_id"].startswith("RJ01-WTG")
    assert len(rec["tags"]) == 5
    assert parse_ts(rec["event_ts"]) == FIXED_START
    assert r.faults == {} and r.clean_reading_count == len(r.readings)


def test_deterministic_for_same_inputs(site):
    a, b = _sim(site, fault_rate=0.1), _sim(site, fault_rate=0.1)
    assert a.readings == b.readings and a.faults == b.faults


def test_seed_changes_output(site):
    assert _sim(site, seed="a").readings != _sim(site, seed="b").readings


def test_derive_seed_is_stable():
    # Must not depend on PYTHONHASHSEED -- reruns rely on reproducing reading ids.
    assert derive_seed("RJ01", FIXED_START, "normal") == derive_seed("RJ01", FIXED_START, "normal")
    assert derive_seed("RJ01", FIXED_START, "normal") != derive_seed("TN01", FIXED_START, "normal")


def test_reading_ids_unique_without_faults(site):
    r = _sim(site, minutes=120)
    ids = [x["reading_id"] for x in r.readings]
    assert len(ids) == len(set(ids))


def test_fault_accounting(site):
    r = _sim(site, minutes=240, fault_rate=0.2)
    assert sum(r.faults.values()) > 0
    ids = [x["reading_id"] for x in r.readings]
    assert len(ids) - len(set(ids)) == r.faults["duplicate"]
    bad_ts = sum(1 for x in r.readings if x["event_ts"] == "not-a-timestamp")
    assert bad_ts == r.faults["bad_timestamp"]
    quarantined = sum(n for k, n in r.faults.items() if k != "duplicate")
    assert r.clean_reading_count == len(r.readings) - quarantined - r.faults["duplicate"]


def test_storm_triggers_cut_out(site):
    r = _sim(site, minutes=120, scenario="storm")
    second_half = [
        x for x in r.readings if parse_ts(x["event_ts"]) >= FIXED_START + timedelta(minutes=70)
    ]
    assert any(x["status_code"] == "CUT_OUT" for x in second_half)
    assert all(x["power_output_kw"] == 0 for x in second_half if x["status_code"] == "CUT_OUT")


def test_heatwave_raises_gearbox_temperature(site):
    normal = _sim(site, minutes=120, scenario="normal")
    hot = _sim(site, minutes=120, scenario="heatwave")

    def avg(r):
        return sum(x["gearbox_oil_temp_c"] for x in r.readings) / len(r.readings)

    assert avg(hot) > avg(normal) + 10


def test_maintenance_scenario_emits_cdc_and_zero_power(site):
    r = _sim(site, minutes=60, scenario="maintenance")
    assert [e["status"] for e in r.cdc_events] == ["maintenance", "active"]
    tid = r.cdc_events[0]["turbine_id"]
    readings = [x for x in r.readings if x["turbine_id"] == tid]
    assert all(x["status_code"] == "MAINTENANCE" and x["power_output_kw"] == 0 for x in readings)


def test_repower_scenario_uprates_capacity(site):
    r = _sim(site, scenario="repower")
    (event,) = r.cdc_events
    original = next(t for t in site.turbines if t.turbine_id == event["turbine_id"])
    assert event["capacity_mw"] == pytest.approx(original.capacity_mw * 1.1)


def test_master_data_seeding_events_are_ordered(site):
    r = _sim(site, scenario="maintenance", seed_master_data=True)
    assert len(r.cdc_events) == len(site.turbines) + 2
    seqs = [e["change_seq"] for e in r.cdc_events]
    assert len(seqs) == len(set(seqs))
    assert all(e["operation"] == "UPSERT" for e in r.cdc_events)


def test_status_maintenance_turbine_never_produces(sites):
    mh = sites["MH01"]
    r = _sim(mh, minutes=60)
    assert all(x["power_output_kw"] == 0 for x in r.readings if x["turbine_id"] == "MH01-WTG01")


@pytest.mark.parametrize(
    "kwargs",
    [{"scenario": "tornado"}, {"minutes": 0}, {"interval_seconds": -1}, {"fault_rate": 0.9}],
)
def test_rejects_bad_arguments(site, kwargs):
    with pytest.raises(ValueError):
        _sim(site, **kwargs)


def test_resolve_start():
    now = datetime(2026, 11, 12, 10, 37, 42, tzinfo=timezone.utc)
    assert resolve_start(None, 60, now) == datetime(2026, 11, 12, 9, 37, tzinfo=timezone.utc)
    assert resolve_start("2026-11-10T06:00:30Z", 60) == FIXED_START


def test_write_landing_files_layout(site, tmp_path):
    r = _sim(site, minutes=180, scenario="repower", seed_master_data=True)
    assert not master_data_seeded(tmp_path, "RJ01")
    files = write_landing_files(r, tmp_path, "RJ01", FIXED_START, "run42")
    sensor_files = [f for f in files if f.parent == tmp_path / "sensors" / "RJ01"]
    cdc_files = [f for f in files if f.parent == tmp_path / "turbine_cdc" / "RJ01"]
    assert len(sensor_files) == 2  # 720 readings at 500 per file
    assert len(cdc_files) == 1
    lines = sum(len(f.read_text().splitlines()) for f in sensor_files)
    assert lines == len(r.readings)
    assert json.loads(cdc_files[0].read_text().splitlines()[0])["turbine_id"].startswith("RJ01")
    assert master_data_seeded(tmp_path, "RJ01")
