"""After the pipeline update: compare each site's manifest with what the tables hold.

Fails the task (and so the job run) if any check fails.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta

from jade_marmot_wind.jobs._runtime import manifest_dir
from jade_marmot_wind.naming import Names
from jade_marmot_wind.simulator import format_ts, parse_ts
from jade_marmot_wind.validation import check_site_run, summarise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--run-id", required=True)
    return p.parse_args(argv)


def _scalar(spark, sql: str, **params) -> int:
    return int(spark.sql(sql, args=params).collect()[0][0])


def observe(spark, names: Names, manifest: dict) -> dict:
    site = manifest["site_id"]
    start = parse_ts(manifest["window_start"])
    end = start + timedelta(minutes=manifest["minutes"])
    window = {"site": site, "start": format_ts(start), "end": format_ts(end)}
    return {
        "silver_in_window": _scalar(
            spark,
            f"SELECT count(DISTINCT reading_id) FROM {names.silver_sensor_readings} "
            "WHERE site_id = :site AND event_ts >= :start::timestamp AND event_ts < :end::timestamp",
            **window,
        ),
        "quarantined_for_run": _scalar(
            spark,
            f"SELECT count(*) FROM {names.silver_quarantine} "
            "WHERE sim_run_id = :run AND substring(turbine_id, 1, 4) = :site",
            run=manifest["run_id"],
            site=site,
        ),
        "dim_turbine_current": _scalar(
            spark,
            f"SELECT count(*) FROM {names.dim_turbine} WHERE site_id = :site AND __END_AT IS NULL",
            site=site,
        ),
        "hourly_rows": _scalar(
            spark,
            f"SELECT count(*) FROM {names.hourly_metrics} WHERE site_id = :site "
            "AND hour_start >= date_trunc('HOUR', :start::timestamp) AND hour_start < :end::timestamp",
            **window,
        ),
        "kpi_rows": _scalar(
            spark,
            f"SELECT count(*) FROM {names.site_daily_kpis} WHERE site_id = :site "
            "AND metric_date BETWEEN to_date(:start::timestamp) AND to_date(:end::timestamp)",
            **window,
        ),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    names = Names(catalog=args.catalog)

    manifests = sorted(manifest_dir(args.landing_root, args.run_id).glob("*.json"))
    if not manifests:
        raise SystemExit(f"no simulation manifests for run {args.run_id}")

    checks = []
    for path in manifests:
        manifest = json.loads(path.read_text())
        observed = observe(spark, names, manifest)
        print(f"{manifest['site_id']}: observed {observed}")
        checks.extend(check_site_run(manifest, observed))

    ok, report = summarise(checks)
    print(report)
    if not ok:
        raise SystemExit("validation failed")


if __name__ == "__main__":
    main()
