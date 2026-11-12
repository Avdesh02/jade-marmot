import pytest

from jade_marmot_wind.naming import (
    Names,
    catalog_name,
    check_identifier,
    model_name_from_folder,
    resource_name,
)


@pytest.mark.parametrize(
    "folder, expected",
    [
        ("jade-marmot", "jade_marmot"),
        ("Jade Marmot", "jade_marmot"),
        ("my--repo__x", "my_repo_x"),
        ("-edge-", "edge"),
        ("2024-wind", "m_2024_wind"),
    ],
)
def test_model_name_from_folder(folder, expected):
    assert model_name_from_folder(folder) == expected


def test_model_name_rejects_empty():
    with pytest.raises(ValueError):
        model_name_from_folder("---")


def test_catalog_and_resource_names_carry_model_name():
    assert catalog_name("jade_marmot", "dev") == "jade_marmot_dev"
    assert (
        resource_name("jade_marmot", "dev", "wind_telemetry_sdp")
        == "jade_marmot_dev_wind_telemetry_sdp"
    )


def test_names_are_fully_qualified():
    n = Names("jade_marmot_dev")
    assert n.bronze_sensor_readings == "jade_marmot_dev.bronze.sensor_readings_raw"
    assert n.dim_turbine == "jade_marmot_dev.silver.dim_turbine"
    assert n.site_daily_kpis == "jade_marmot_dev.gold.site_daily_kpis"
    assert n.alert_digest == "jade_marmot_dev.ops.alert_digest"


def test_names_from_conf_uses_defaults():
    conf = {"wind.catalog": "jade_marmot_dev", "wind.schema.gold": "serving"}
    n = Names.from_conf(lambda k, d: conf.get(k, d))
    assert n.bronze == "bronze"
    assert n.turbine_alerts == "jade_marmot_dev.serving.turbine_alerts"


@pytest.mark.parametrize("bad", ["", "jade-marmot", "x; DROP TABLE y", "Upper"])
def test_names_reject_unsafe_identifiers(bad):
    with pytest.raises(ValueError):
        Names(bad)
    with pytest.raises(ValueError):
        check_identifier(bad)
