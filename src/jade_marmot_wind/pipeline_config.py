"""Pipeline configuration, read from the SDP pipeline's ``configuration`` block.

These are the pipeline's **static** parameters -- fixed per bundle target and set in
``resources/wind_telemetry.pipeline.yml``:

    wind.catalog                     target catalog, e.g. jade_marmot_dev
    wind.sites                       enabled sites, comma-separated or "all"
    wind.landing_root                UC Volume path the simulator / site gateways write to
    wind.silver.dedupe_watermark     how long a reading_id is remembered for de-duplication
    wind.threshold.<metric>.<level>  alert thresholds, level is warning|critical

Per-run (dynamic) choices -- which sites, which scenario, full refresh or not -- live in
the job, which decides what lands and which flows refresh.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from jade_marmot_wind.naming import Names
from jade_marmot_wind.sites import Site, load_sites, select_sites
from jade_marmot_wind.transforms import Threshold, thresholds_from_conf


@dataclass(frozen=True)
class PipelineConfig:
    names: Names
    landing_root: str
    all_sites: dict[str, Site]
    enabled_sites: list[Site]
    dedupe_watermark: str
    thresholds: tuple[Threshold, ...]

    @classmethod
    def from_conf(cls, get: Callable[[str, str], str]) -> PipelineConfig:
        landing_root = get("wind.landing_root", "").rstrip("/")
        if not get("wind.catalog", "") or not landing_root:
            raise ValueError("pipeline configuration must set wind.catalog and wind.landing_root")
        names = Names.from_conf(get)
        all_sites = load_sites()
        return cls(
            names=names,
            landing_root=landing_root,
            all_sites=all_sites,
            enabled_sites=select_sites(all_sites, get("wind.sites", "all")),
            dedupe_watermark=get("wind.silver.dedupe_watermark", "3 days"),
            thresholds=thresholds_from_conf(get),
        )

    @classmethod
    def from_spark(cls, spark) -> PipelineConfig:
        return cls.from_conf(lambda key, default: spark.conf.get(key, default))

    def sensor_path(self, site_id: str) -> str:
        return f"{self.landing_root}/sensors/{site_id}"

    def cdc_path(self, site_id: str) -> str:
        return f"{self.landing_root}/turbine_cdc/{site_id}"

    @property
    def enabled_site_ids(self) -> list[str]:
        return [s.site_id for s in self.enabled_sites]
