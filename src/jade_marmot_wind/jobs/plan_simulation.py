"""Task 1 of the orchestration job: resolve which sites this run covers.

Combines the static ``--enabled-sites`` (bundle variable, fixed per target) with the
dynamic ``--site-ids`` (job parameter, chosen per run) and publishes the result as the
``sites`` task value. The ``simulate_sites`` for_each task iterates over it.

It also makes sure every enabled site has its landing folders, so the pipeline's per-site
Auto Loader flows have a directory to list even for sites this run doesn't simulate.
"""

from __future__ import annotations

import argparse
import sys

from jade_marmot_wind.jobs._runtime import set_task_value
from jade_marmot_wind.simulator import SCENARIOS, cdc_dir, sensor_dir
from jade_marmot_wind.sites import load_sites, select_sites


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--enabled-sites", required=True, help="static: sites enabled for the target")
    p.add_argument("--site-ids", default="all", help="dynamic: 'all' or comma-separated ids")
    p.add_argument("--landing-root", required=True)
    p.add_argument("--scenario", default="normal", choices=SCENARIOS)
    return p.parse_args(argv)


def run(args: argparse.Namespace) -> list[dict]:
    all_sites = load_sites()
    enabled = select_sites(all_sites, args.enabled_sites)
    for site in enabled:
        sensor_dir(args.landing_root, site.site_id).mkdir(parents=True, exist_ok=True)
        cdc_dir(args.landing_root, site.site_id).mkdir(parents=True, exist_ok=True)

    selected = select_sites(all_sites, args.enabled_sites, args.site_ids)
    inputs = [s.to_task_input() for s in selected]
    set_task_value("sites", inputs)
    set_task_value("site_ids", ",".join(s.site_id for s in selected))
    print(
        f"scenario={args.scenario}: simulating {len(inputs)} site(s): {[i['site_id'] for i in inputs]}"
    )
    return inputs


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    main()
