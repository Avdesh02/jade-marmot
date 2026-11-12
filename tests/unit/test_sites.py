import json

import pytest

from jade_marmot_wind.sites import load_sites, parse_site_ids, select_sites


def test_bundled_site_config_is_consistent(sites):
    assert list(sites) == ["RJ01", "TN01", "MH01", "GJ01", "KA01"]
    for site in sites.values():
        assert site.turbines
        for t in site.turbines:
            # silver's site_matches_turbine rule depends on this prefix convention
            assert t.turbine_id.startswith(f"{site.site_id}-WTG")


def test_legacy_turbine_ids_are_preserved(sites):
    legacy = {t.legacy_turbine_id for s in sites.values() for t in s.turbines}
    assert {"TURB-001", "TURB-002", "TURB-003", "TURB-004", "TURB-005"} <= legacy


def test_site_capacity_and_row(site):
    assert site.capacity_mw == pytest.approx(9.2)
    row = site.to_row()
    assert row["turbine_count"] == 4 and "turbines" not in row
    assert site.to_task_input() == {"site_id": "RJ01", "turbine_count": 4}


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        ("all", None),
        (" ALL ", None),
        ("rj01", ["RJ01"]),
        ("RJ01, tn01,RJ01", ["RJ01", "TN01"]),
    ],
)
def test_parse_site_ids(value, expected):
    assert parse_site_ids(value) == expected


def test_select_sites_static_and_dynamic(sites):
    assert [s.site_id for s in select_sites(sites, "all")] == list(sites)
    assert [s.site_id for s in select_sites(sites, "RJ01,TN01,GJ01", "all")] == [
        "RJ01",
        "TN01",
        "GJ01",
    ]
    assert [s.site_id for s in select_sites(sites, "RJ01,TN01,GJ01", "gj01")] == ["GJ01"]


def test_select_sites_rejects_requests_outside_enabled(sites):
    with pytest.raises(ValueError, match="not enabled"):
        select_sites(sites, "RJ01,TN01", "KA01")


def test_select_sites_rejects_unknown_enabled(sites):
    with pytest.raises(ValueError, match="not in site config"):
        select_sites(sites, "RJ01,XX01")


def test_load_sites_rejects_duplicate_turbines(tmp_path):
    t = {
        "turbine_id": "AA01-WTG01",
        "model": "m",
        "capacity_mw": 1.0,
        "hub_height_m": 1,
        "rotor_diameter_m": 1,
        "commission_date": "2020-01-01",
        "status": "active",
    }
    site = {
        "site_name": "x",
        "state": "x",
        "timezone": "UTC",
        "latitude": 0,
        "longitude": 0,
        "wind_factor": 1,
        "ambient_temp_c": 20,
        "turbines": [t],
    }
    cfg = tmp_path / "sites.json"
    cfg.write_text(
        json.dumps({"sites": [{**site, "site_id": "AA01"}, {**site, "site_id": "AA02"}]})
    )
    with pytest.raises(ValueError, match="unique"):
        load_sites(cfg)
