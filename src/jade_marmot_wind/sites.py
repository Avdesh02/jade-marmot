"""Site master data and site selection.

Two layers of parameterisation meet here:

* **static** -- ``enabled_sites``: the sites a deployment target serves. Fixed at deploy
  time by the bundle variable ``sites`` (dev runs three parks, prod runs all five).
* **dynamic** -- ``requested``: the ``site_ids`` job parameter, chosen per run (``all`` or
  a comma-separated subset). It must be a subset of the enabled sites.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

SITES_CONFIG = Path(__file__).parent / "config" / "sites.json"
ALL = "all"


@dataclass(frozen=True)
class Turbine:
    turbine_id: str
    model: str
    capacity_mw: float
    hub_height_m: float
    rotor_diameter_m: float
    commission_date: str
    status: str
    legacy_turbine_id: str | None = None


@dataclass(frozen=True)
class Site:
    site_id: str
    site_name: str
    state: str
    timezone: str
    latitude: float
    longitude: float
    wind_factor: float
    ambient_temp_c: float
    turbines: tuple[Turbine, ...]

    @property
    def capacity_mw(self) -> float:
        return round(sum(t.capacity_mw for t in self.turbines), 3)

    def to_task_input(self) -> dict:
        """The element a for_each iteration receives as ``{{input}}``. Kept small: the
        for_each inputs array is capped in size, and iterations reload the site anyway."""
        return {"site_id": self.site_id, "turbine_count": len(self.turbines)}

    def to_row(self) -> dict:
        """Flat row for the ``dim_site`` table."""
        row = {k: v for k, v in asdict(self).items() if k != "turbines"}
        row["turbine_count"] = len(self.turbines)
        row["capacity_mw"] = self.capacity_mw
        return row


def load_sites(path: Path | str | None = None) -> dict[str, Site]:
    raw = json.loads(Path(path or SITES_CONFIG).read_text(encoding="utf-8"))
    sites: dict[str, Site] = {}
    for s in raw["sites"]:
        turbines = tuple(Turbine(**t) for t in s.pop("turbines"))
        site = Site(**s, turbines=turbines)
        if site.site_id in sites:
            raise ValueError(f"duplicate site_id {site.site_id!r} in {path or SITES_CONFIG}")
        sites[site.site_id] = site
    turbine_ids = [t.turbine_id for s in sites.values() for t in s.turbines]
    if len(turbine_ids) != len(set(turbine_ids)):
        raise ValueError("turbine_id must be unique across all sites")
    return sites


def parse_site_ids(value: str | None) -> list[str] | None:
    """``"all"``/empty -> None (meaning every enabled site); otherwise a de-duplicated list
    in the order given."""
    if value is None or value.strip().lower() in ("", ALL):
        return None
    ids = [v.strip().upper() for v in value.split(",") if v.strip()]
    return list(dict.fromkeys(ids))


def select_sites(
    all_sites: dict[str, Site], enabled: str | None, requested: str | None = None
) -> list[Site]:
    enabled_ids = parse_site_ids(enabled) or list(all_sites)
    unknown = [s for s in enabled_ids if s not in all_sites]
    if unknown:
        raise ValueError(f"enabled sites not in site config: {unknown}")

    requested_ids = parse_site_ids(requested) or enabled_ids
    not_enabled = [s for s in requested_ids if s not in enabled_ids]
    if not_enabled:
        raise ValueError(
            f"requested sites {not_enabled} are not enabled for this target "
            f"(enabled: {enabled_ids})"
        )
    return [all_sites[s] for s in requested_ids]
