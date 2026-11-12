from jade_marmot_wind.validation import check_site_run, summarise

MANIFEST = {
    "site_id": "RJ01",
    "turbines": ["RJ01-WTG01", "RJ01-WTG02"],
    "minutes": 60,
    "interval_seconds": 60,
    "expected_clean_readings": 115,
    "expected_quarantined": 3,
}
GOOD = {
    "silver_in_window": 115,
    "quarantined_for_run": 3,
    "dim_turbine_current": 2,
    "hourly_rows": 4,
    "kpi_rows": 1,
}


def test_all_pass():
    ok, report = summarise(check_site_run(MANIFEST, GOOD))
    assert ok and "FAIL" not in report


def test_silver_may_include_earlier_runs_of_same_window():
    ok, _ = summarise(check_site_run(MANIFEST, {**GOOD, "silver_in_window": 120}))
    assert ok


def test_each_failure_is_reported():
    bad = {
        "silver_in_window": 100,
        "quarantined_for_run": 0,
        "dim_turbine_current": 3,
        "hourly_rows": 0,
        "kpi_rows": 0,
    }
    checks = check_site_run(MANIFEST, bad)
    assert not any(c.passed for c in checks)
    ok, report = summarise(checks)
    assert not ok and report.count("[FAIL]") == 5


def test_silver_above_max_possible_fails():
    ok, _ = summarise(check_site_run(MANIFEST, {**GOOD, "silver_in_window": 121}))
    assert not ok
