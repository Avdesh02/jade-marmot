"""Post-pipeline checks: did what the simulator wrote come out the other end?

The comparisons live here as pure functions over plain numbers so they're unit-testable;
``jobs/validate_outputs.py`` gathers the numbers with Spark and calls them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def check_site_run(manifest: dict, observed: dict) -> list[Check]:
    """``observed`` keys: silver_in_window, quarantined_for_run, dim_turbine_current,
    hourly_rows, kpi_rows."""
    site = manifest["site_id"]
    expected_clean = manifest["expected_clean_readings"]
    max_possible = len(manifest["turbines"]) * (
        manifest["minutes"] * 60 // manifest["interval_seconds"]
    )
    silver = observed["silver_in_window"]
    checks = [
        # >= rather than ==: an earlier run over the same window (same reading ids) may
        # have contributed rows that this run's faulted copies would otherwise replace.
        Check(
            f"{site}: silver readings",
            expected_clean <= silver <= max_possible,
            f"{silver} distinct readings in window, expected {expected_clean}..{max_possible}",
        ),
        Check(
            f"{site}: quarantine",
            observed["quarantined_for_run"] == manifest["expected_quarantined"],
            f"{observed['quarantined_for_run']} quarantined, "
            f"expected {manifest['expected_quarantined']}",
        ),
        Check(
            f"{site}: dim_turbine current rows",
            observed["dim_turbine_current"] == len(manifest["turbines"]),
            f"{observed['dim_turbine_current']} current versions, "
            f"expected {len(manifest['turbines'])}",
        ),
        Check(
            f"{site}: hourly metrics",
            observed["hourly_rows"] > 0 or expected_clean == 0,
            f"{observed['hourly_rows']} hourly rows in window",
        ),
        Check(
            f"{site}: daily KPIs",
            observed["kpi_rows"] > 0 or expected_clean == 0,
            f"{observed['kpi_rows']} KPI rows for window dates",
        ),
    ]
    return checks


def summarise(checks: list[Check]) -> tuple[bool, str]:
    lines = [f"[{'PASS' if c.passed else 'FAIL'}] {c.name}: {c.detail}" for c in checks]
    ok = all(c.passed for c in checks)
    return ok, "\n".join(lines)
