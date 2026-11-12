"""Test-data simulator: synthetic SCADA telemetry and turbine master-data CDC events.

Pure Python (no Spark) so it's fast, deterministic and unit-testable. It writes JSON-lines
files into the landing Volume, exactly where real site gateways would drop them:

    {landing_root}/sensors/{site_id}/{run_id}_{start}_{part}.json      -> Auto Loader, append flows
    {landing_root}/turbine_cdc/{site_id}/{run_id}_{start}_cdc.json     -> AUTO CDC flows

Directories are named by bare site id (not ``site_id=XX``) so Auto Loader doesn't try to
infer a partition column that clashes with the ``site_id`` field in the payload.

Scenarios shape the physics and the CDC stream:

* ``normal``      diurnal wind, occasional gusts
* ``storm``       wind ramps past the 25 m/s cut-out in the second half of the window
* ``heatwave``    +10 C ambient, gearbox/generator temperatures push into alert bands
* ``maintenance`` one turbine goes to maintenance for the window (two CDC events)
* ``repower``     one turbine is uprated by 10% at the start of the window (one CDC event)

``fault_rate`` injects bad records the silver layer must quarantine, plus exact duplicates
it must de-duplicate.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jade_marmot_wind.sites import Site, Turbine

SCENARIOS = ("normal", "storm", "heatwave", "maintenance", "repower")

CUT_IN_MPS = 3.0
RATED_MPS = 12.0
CUT_OUT_MPS = 25.0
TIP_SPEED_MPS = 80.0

# Every fault kind except "duplicate" must fail at least one silver quality rule.
FAULT_KINDS = (
    "null_wind_speed",
    "negative_power",
    "bad_timestamp",
    "temperature_spike",
    "site_mismatch",
    "duplicate",
)

READINGS_PER_FILE = 500


@dataclass
class SimulationResult:
    readings: list[dict]
    cdc_events: list[dict]
    faults: Counter = field(default_factory=Counter)

    @property
    def clean_reading_count(self) -> int:
        """Readings the silver layer should keep: everything minus faulted rows and
        minus the extra copies made by duplicate injection."""
        quarantined = sum(n for kind, n in self.faults.items() if kind != "duplicate")
        return len(self.readings) - quarantined - self.faults["duplicate"]


def parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def format_ts(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_start(start_ts: str | None, minutes: int, now: datetime | None = None) -> datetime:
    """Explicit start, or the window ending at the current minute so data looks live."""
    if start_ts:
        return parse_ts(start_ts).replace(second=0, microsecond=0)
    now = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    return now - timedelta(minutes=minutes)


def derive_seed(site_id: str, start: datetime, scenario: str, seed: str | None = None) -> int:
    """Stable across processes (unlike ``hash()``), so a rerun reproduces the same data
    and the same reading ids -- which is what lets silver de-duplicate reruns."""
    material = seed if seed else f"{site_id}|{format_ts(start)}|{scenario}"
    return int.from_bytes(hashlib.sha256(material.encode()).digest()[:8], "big")


def rated_rpm(turbine: Turbine) -> float:
    return TIP_SPEED_MPS * 60 / (math.pi * turbine.rotor_diameter_m)


def power_kw(turbine: Turbine, wind_mps: float) -> float:
    """Idealised power curve: cubic between cut-in and rated, flat to cut-out, then 0."""
    rated_kw = turbine.capacity_mw * 1000
    if wind_mps < CUT_IN_MPS or wind_mps >= CUT_OUT_MPS:
        return 0.0
    if wind_mps >= RATED_MPS:
        return rated_kw
    frac = (wind_mps**3 - CUT_IN_MPS**3) / (RATED_MPS**3 - CUT_IN_MPS**3)
    return rated_kw * frac


def _diurnal(hour_local: float) -> float:
    # Peaks mid-afternoon, troughs pre-dawn.
    return 1.0 + 0.35 * math.sin((hour_local - 9) * math.pi / 12)


def _site_utc_offset_hours(site: Site) -> float:
    # All bundled sites are Asia/Kolkata; a fixed offset keeps the simulator free of tz data.
    return 5.5 if site.timezone == "Asia/Kolkata" else 0.0


def _pick_turbine(site: Site, rng: random.Random, statuses=("active",)) -> Turbine:
    candidates = [t for t in site.turbines if t.status in statuses] or list(site.turbines)
    return rng.choice(candidates)


def _turbine_record(site: Site, t: Turbine, **overrides) -> dict:
    rec = {
        "turbine_id": t.turbine_id,
        "site_id": site.site_id,
        "model": t.model,
        "capacity_mw": t.capacity_mw,
        "hub_height_m": t.hub_height_m,
        "rotor_diameter_m": t.rotor_diameter_m,
        "commission_date": t.commission_date,
        "status": t.status,
    }
    rec.update(overrides)
    return rec


def cdc_events(
    site: Site,
    scenario: str,
    start: datetime,
    minutes: int,
    rng: random.Random,
    seed_master_data: bool,
) -> tuple[list[dict], set[str]]:
    """Master-data change events, plus the ids of turbines in maintenance for the window."""
    events: list[dict] = []

    def emit(rec: dict, changed_at: datetime, operation: str = "UPSERT") -> None:
        events.append(
            {
                **rec,
                "operation": operation,
                # Unique and monotonic per turbine; AUTO CDC sequences by changed_at, this
                # is kept for audit and tie-break debugging.
                "change_seq": int(changed_at.timestamp() * 1000) * 100 + len(events),
                "changed_at": format_ts(changed_at),
            }
        )

    if seed_master_data:
        for t in site.turbines:
            commissioned = datetime.fromisoformat(t.commission_date).replace(tzinfo=timezone.utc)
            emit(_turbine_record(site, t), commissioned)

    in_maintenance = {t.turbine_id for t in site.turbines if t.status == "maintenance"}
    if scenario == "maintenance":
        t = _pick_turbine(site, rng)
        in_maintenance.add(t.turbine_id)
        emit(_turbine_record(site, t, status="maintenance"), start)
        emit(_turbine_record(site, t, status="active"), start + timedelta(minutes=minutes))
    elif scenario == "repower":
        t = _pick_turbine(site, rng)
        emit(
            _turbine_record(
                site, t, capacity_mw=round(t.capacity_mw * 1.1, 3), model=f"{t.model}-U"
            ),
            start,
        )
    return events, in_maintenance


def _reading(
    site: Site,
    t: Turbine,
    ts: datetime,
    progress: float,
    scenario: str,
    in_maintenance: bool,
    wind: float,
    direction: float,
    interval_s: int,
    tag_count: int,
    rng: random.Random,
    run_id: str,
) -> dict:
    hour_local = (ts.hour + ts.minute / 60 + _site_utc_offset_hours(site)) % 24
    heat = 10.0 if scenario == "heatwave" else 0.0
    ambient = site.ambient_temp_c + heat + 4 * math.sin((hour_local - 9) * math.pi / 12)
    ambient += rng.gauss(0, 0.6)

    rated_kw = t.capacity_mw * 1000
    if in_maintenance:
        status, kw, rpm, pitch = "MAINTENANCE", 0.0, 0.0, 90.0
    elif wind >= CUT_OUT_MPS:
        status, kw, rpm, pitch = "CUT_OUT", 0.0, rng.uniform(0, 1.0), 90.0
    elif wind < CUT_IN_MPS:
        status, kw, rpm, pitch = "IDLE", 0.0, rng.uniform(0, 2.0), 12.0
    else:
        status = "RUNNING"
        kw = min(rated_kw, max(0.0, power_kw(t, wind) * rng.uniform(0.97, 1.02)))
        rpm = rated_rpm(t) * min(wind / RATED_MPS, 1.0) * rng.uniform(0.98, 1.02)
        pitch = 0.0 if wind < RATED_MPS else min(25.0, (wind - RATED_MPS) * 1.8)

    load = kw / rated_kw if rated_kw else 0.0
    storm_vib = 4.5 if scenario == "storm" and progress >= 0.5 else 0.0
    rec = {
        "reading_id": f"{t.turbine_id}-{ts:%Y%m%dT%H%M%S}",
        "site_id": site.site_id,
        "turbine_id": t.turbine_id,
        "event_ts": format_ts(ts),
        "sample_interval_s": interval_s,
        "wind_speed_mps": round(wind, 2),
        "wind_direction_deg": round(direction % 360, 1),
        "power_output_kw": round(kw, 2),
        "rotor_rpm": round(rpm, 2),
        "pitch_angle_deg": round(pitch, 1),
        "ambient_temp_c": round(ambient, 2),
        "nacelle_temp_c": round(ambient + 8 + 6 * load + rng.gauss(0, 0.5), 2),
        "gearbox_oil_temp_c": round(ambient + 18 + 22 * load + 1.2 * heat + rng.gauss(0, 1), 2),
        "generator_temp_c": round(ambient + 28 + 45 * load + 1.5 * heat + rng.gauss(0, 1.5), 2),
        "pressure_hpa": round(1010 - (15 if scenario == "storm" else 0) + rng.gauss(0, 2), 2),
        "vibration_mm_s": round(max(0.1, 1.2 + 3.0 * load + storm_vib + rng.gauss(0, 0.3)), 2),
        "status_code": status,
        "tags": {f"tag_{i:03d}": round(rng.uniform(0, 100), 2) for i in range(1, tag_count + 1)},
        "scenario": scenario,
        "sim_run_id": run_id,
    }
    return rec


def _inject_fault(rec: dict, kind: str) -> dict:
    rec = dict(rec)
    if kind == "null_wind_speed":
        rec["wind_speed_mps"] = None
    elif kind == "negative_power":
        rec["power_output_kw"] = -750.0
    elif kind == "bad_timestamp":
        rec["event_ts"] = "not-a-timestamp"
    elif kind == "temperature_spike":
        rec["gearbox_oil_temp_c"] = 999.0
    elif kind == "site_mismatch":
        rec["site_id"] = "ZZ99"
    return rec


def simulate_site(
    site: Site,
    *,
    start: datetime,
    minutes: int,
    scenario: str = "normal",
    interval_seconds: int = 60,
    fault_rate: float = 0.0,
    tag_count: int = 8,
    seed: str | None = None,
    run_id: str = "local",
    seed_master_data: bool = False,
) -> SimulationResult:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; expected one of {SCENARIOS}")
    if minutes <= 0 or interval_seconds <= 0:
        raise ValueError("minutes and interval_seconds must be positive")
    if not 0.0 <= fault_rate <= 0.5:
        raise ValueError("fault_rate must be between 0 and 0.5")

    rng = random.Random(derive_seed(site.site_id, start, scenario, seed))
    events, in_maintenance = cdc_events(site, scenario, start, minutes, rng, seed_master_data)

    steps = (minutes * 60) // interval_seconds
    readings: list[dict] = []
    faults: Counter = Counter()

    for t in site.turbines:
        # Each turbine gets its own AR(1) wind process around the site's diurnal mean.
        wind = 7.5 * site.wind_factor + rng.gauss(0, 1.0)
        direction = rng.uniform(0, 360)
        for step in range(steps):
            ts = start + timedelta(seconds=step * interval_seconds)
            progress = step / max(steps - 1, 1)
            hour_local = (ts.hour + ts.minute / 60 + _site_utc_offset_hours(site)) % 24
            mean = 7.5 * site.wind_factor * _diurnal(hour_local)
            if scenario == "storm" and progress >= 0.5:
                mean = 27.0 + 6.0 * (progress - 0.5)
            wind = max(0.0, wind + 0.25 * (mean - wind) + rng.gauss(0, 0.8))
            direction += rng.gauss(0, 3)

            rec = _reading(
                site, t, ts, progress, scenario, t.turbine_id in in_maintenance,
                wind, direction, interval_seconds, tag_count, rng, run_id,
            )  # fmt: skip
            if fault_rate and rng.random() < fault_rate:
                kind = rng.choice(FAULT_KINDS)
                faults[kind] += 1
                if kind == "duplicate":
                    readings.append(rec)
                else:
                    rec = _inject_fault(rec, kind)
            readings.append(rec)

    return SimulationResult(readings=readings, cdc_events=events, faults=faults)


def sensor_dir(landing_root: str | Path, site_id: str) -> Path:
    return Path(landing_root) / "sensors" / site_id


def cdc_dir(landing_root: str | Path, site_id: str) -> Path:
    return Path(landing_root) / "turbine_cdc" / site_id


def master_data_seeded(landing_root: str | Path, site_id: str) -> bool:
    d = cdc_dir(landing_root, site_id)
    return d.is_dir() and any(p.suffix == ".json" for p in d.iterdir())


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # One write call: UC Volumes are object-store backed, and a single PUT means Auto
    # Loader never lists a half-written file.
    path.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))


def write_landing_files(
    result: SimulationResult, landing_root: str | Path, site_id: str, start: datetime, run_id: str
) -> list[Path]:
    stem = f"{run_id}_{start:%Y%m%dT%H%M}"
    written: list[Path] = []
    for part, i in enumerate(range(0, len(result.readings), READINGS_PER_FILE)):
        path = sensor_dir(landing_root, site_id) / f"{stem}_{part:03d}.json"
        _write_jsonl(path, result.readings[i : i + READINGS_PER_FILE])
        written.append(path)
    if result.cdc_events:
        path = cdc_dir(landing_root, site_id) / f"{stem}_cdc.json"
        _write_jsonl(path, result.cdc_events)
        written.append(path)
    return written
