"""Naming standard.

Every Databricks object this project creates carries ``{{model_name}}`` -- the git folder
name, normalised to a Unity Catalog identifier (``jade-marmot`` -> ``jade_marmot``):

* catalog            ``{model_name}_{target}``                      e.g. ``jade_marmot_dev``
* jobs / pipelines   ``{model_name}_{target}_{purpose}``            e.g. ``jade_marmot_dev_wind_telemetry_sdp``
* workspace folder   ``/Workspace/Users/<me>/{model_name}/...``
* tables             ``{catalog}.{layer}.{table}`` -- the catalog already carries the name

The bundle (``databricks.yml``) applies the same rules through ``${var.model_name}``;
``tests/unit/test_bundle_config.py`` checks the two agree.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def model_name_from_folder(folder: str) -> str:
    """Normalise a git folder name into a UC-safe identifier.

    Hyphens and other punctuation become ``_`` because UC identifiers can't contain them
    unquoted, and quoting every reference is a footgun.
    """
    name = re.sub(r"[^0-9A-Za-z]+", "_", folder).strip("_").lower()
    if not name:
        raise ValueError(f"cannot derive a model name from folder {folder!r}")
    if name[0].isdigit():
        name = f"m_{name}"
    return name


def catalog_name(model_name: str, target: str) -> str:
    return f"{model_name}_{target}"


def resource_name(model_name: str, target: str, purpose: str) -> str:
    return f"{model_name}_{target}_{purpose}"


def check_identifier(value: str, what: str = "identifier") -> str:
    """Guard for names that get interpolated into SQL text."""
    if not _IDENTIFIER.match(value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


# Table names, unqualified. Kept here so the pipeline, the validation job and the tests
# can't drift apart.
BRONZE_SENSOR_READINGS = "sensor_readings_raw"
BRONZE_TURBINE_CDC = "turbine_cdc_raw"
SILVER_SENSOR_READINGS = "sensor_readings"
SILVER_QUARANTINE = "sensor_readings_quarantine"
SILVER_DIM_TURBINE = "dim_turbine"
SILVER_TURBINE_CURRENT = "turbine_current"
SILVER_DIM_SITE = "dim_site"
GOLD_TURBINE_ALERTS = "turbine_alerts"
GOLD_HOURLY_METRICS = "turbine_hourly_metrics"
GOLD_SITE_DAILY_KPIS = "site_daily_kpis"
GOLD_FLEET_STATUS = "fleet_status"
OPS_ALERT_DIGEST = "alert_digest"


@dataclass(frozen=True)
class Names:
    """Fully qualified table names for one catalog."""

    catalog: str
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"
    ops: str = "ops"

    def __post_init__(self) -> None:
        for field in ("catalog", "bronze", "silver", "gold", "ops"):
            check_identifier(getattr(self, field), field)

    @classmethod
    def from_conf(cls, get: Callable[[str, str], str]) -> Names:
        """Build from pipeline configuration (``spark.conf.get``-shaped callable)."""
        return cls(
            catalog=get("wind.catalog", ""),
            bronze=get("wind.schema.bronze", "bronze"),
            silver=get("wind.schema.silver", "silver"),
            gold=get("wind.schema.gold", "gold"),
            ops=get("wind.schema.ops", "ops"),
        )

    def _fq(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    @property
    def bronze_sensor_readings(self) -> str:
        return self._fq(self.bronze, BRONZE_SENSOR_READINGS)

    @property
    def bronze_turbine_cdc(self) -> str:
        return self._fq(self.bronze, BRONZE_TURBINE_CDC)

    @property
    def silver_sensor_readings(self) -> str:
        return self._fq(self.silver, SILVER_SENSOR_READINGS)

    @property
    def silver_quarantine(self) -> str:
        return self._fq(self.silver, SILVER_QUARANTINE)

    @property
    def dim_turbine(self) -> str:
        return self._fq(self.silver, SILVER_DIM_TURBINE)

    @property
    def turbine_current(self) -> str:
        return self._fq(self.silver, SILVER_TURBINE_CURRENT)

    @property
    def dim_site(self) -> str:
        return self._fq(self.silver, SILVER_DIM_SITE)

    @property
    def turbine_alerts(self) -> str:
        return self._fq(self.gold, GOLD_TURBINE_ALERTS)

    @property
    def hourly_metrics(self) -> str:
        return self._fq(self.gold, GOLD_HOURLY_METRICS)

    @property
    def site_daily_kpis(self) -> str:
        return self._fq(self.gold, GOLD_SITE_DAILY_KPIS)

    @property
    def fleet_status(self) -> str:
        return self._fq(self.gold, GOLD_FLEET_STATUS)

    @property
    def alert_digest(self) -> str:
        return self._fq(self.ops, OPS_ALERT_DIGEST)
