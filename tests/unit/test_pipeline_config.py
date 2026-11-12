import pytest

from jade_marmot_wind.pipeline_config import PipelineConfig


def _cfg(**overrides):
    conf = {
        "wind.catalog": "jade_marmot_dev",
        "wind.landing_root": "/Volumes/jade_marmot_dev/raw/landing/",
        "wind.sites": "RJ01,GJ01",
        **overrides,
    }
    return PipelineConfig.from_conf(lambda k, d: conf.get(k, d))


def test_static_parameters():
    cfg = _cfg()
    assert cfg.names.catalog == "jade_marmot_dev"
    assert cfg.enabled_site_ids == ["RJ01", "GJ01"]
    assert len(cfg.all_sites) == 5
    assert cfg.dedupe_watermark == "3 days"
    assert cfg.sensor_path("RJ01") == "/Volumes/jade_marmot_dev/raw/landing/sensors/RJ01"
    assert cfg.cdc_path("GJ01") == "/Volumes/jade_marmot_dev/raw/landing/turbine_cdc/GJ01"


def test_all_sites():
    assert len(_cfg(**{"wind.sites": "all"}).enabled_sites) == 5


@pytest.mark.parametrize("missing", ["wind.catalog", "wind.landing_root"])
def test_requires_catalog_and_landing_root(missing):
    with pytest.raises(ValueError, match="must set"):
        _cfg(**{missing: ""})


def test_rejects_unknown_site():
    with pytest.raises(ValueError):
        _cfg(**{"wind.sites": "RJ01,XX01"})
