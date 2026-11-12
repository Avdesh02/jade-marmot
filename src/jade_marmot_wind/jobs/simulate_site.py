"""for_each iteration: simulate one site's telemetry into the landing Volume.

Writes sensor files (and turbine CDC files when the scenario or first-run seeding calls
for them), then a manifest recording what silver and quarantine should end up holding for
this run, which ``validate_outputs`` checks after the pipeline update.
"""

from __future__ import annotations

import argparse
import json
import sys

from jade_marmot_wind.jobs._runtime import manifest_dir, str_to_bool
from jade_marmot_wind.simulator import (
    SCENARIOS,
    format_ts,
    master_data_seeded,
    resolve_start,
    simulate_site,
    write_landing_files,
)
from jade_marmot_wind.sites import load_sites, select_sites


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--site-id", required=True)
    p.add_argument("--enabled-sites", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--run-id", default="", help="set by a parent job; empty = --own-run-id")
    p.add_argument("--own-run-id", default="local", help="this job run's id ({{job.run_id}})")
    p.add_argument("--scenario", default="normal", choices=SCENARIOS)
    p.add_argument("--minutes", type=int, default=60)
    p.add_argument("--interval-seconds", type=int, default=60)
    p.add_argument("--fault-rate", type=float, default=0.0)
    p.add_argument("--tag-count", type=int, default=8)
    p.add_argument("--start-ts", default="", help="ISO-8601 UTC; empty = window ending now")
    p.add_argument("--seed", default="", help="empty = derived from site, start and scenario")
    p.add_argument("--reseed-master-data", default="false")
    return p.parse_args(argv)


def run(args: argparse.Namespace) -> dict:
    # When the orchestrator chains this job it passes its own run id, so the manifest
    # lands where its validate task looks. A standalone run uses its own id.
    args.run_id = args.run_id or args.own_run_id
    (site,) = select_sites(load_sites(), args.enabled_sites, args.site_id)
    start = resolve_start(args.start_ts or None, args.minutes)
    seed_master = str_to_bool(args.reseed_master_data) or not master_data_seeded(
        args.landing_root, site.site_id
    )

    result = simulate_site(
        site,
        start=start,
        minutes=args.minutes,
        scenario=args.scenario,
        interval_seconds=args.interval_seconds,
        fault_rate=args.fault_rate,
        tag_count=args.tag_count,
        seed=args.seed or None,
        run_id=args.run_id,
        seed_master_data=seed_master,
    )
    files = write_landing_files(result, args.landing_root, site.site_id, start, args.run_id)

    manifest = {
        "run_id": args.run_id,
        "site_id": site.site_id,
        "scenario": args.scenario,
        "window_start": format_ts(start),
        "minutes": args.minutes,
        "interval_seconds": args.interval_seconds,
        "turbines": [t.turbine_id for t in site.turbines],
        "readings_written": len(result.readings),
        "expected_clean_readings": result.clean_reading_count,
        "expected_quarantined": sum(n for k, n in result.faults.items() if k != "duplicate"),
        "faults": dict(result.faults),
        "cdc_events": len(result.cdc_events),
        "seeded_master_data": seed_master,
        "files": [str(f) for f in files],
    }
    out = manifest_dir(args.landing_root, args.run_id)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{site.site_id}.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k: v for k, v in manifest.items() if k != "files"}, indent=2))
    return manifest


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    main()
