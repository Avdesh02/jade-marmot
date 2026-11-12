"""Final task: append a per-site alert summary for this run to ``ops.alert_digest``."""

from __future__ import annotations

import argparse
import sys

from jade_marmot_wind.naming import Names
from jade_marmot_wind.sites import parse_site_ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--catalog", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--site-ids", required=True, help="'all' or comma-separated site ids")
    p.add_argument("--lookback-hours", type=int, default=24)
    return p.parse_args(argv)


def build_digest(alerts, run_id: str, site_ids: list[str] | None, lookback_hours: int):
    """Alerts in the ``lookback_hours`` before the newest alert, summarised per site.
    Anchored on the newest alert rather than now() so back-dated simulations still report."""
    from pyspark.sql import functions as F

    if site_ids:
        alerts = alerts.where(F.col("site_id").isin(site_ids))
    horizon = alerts.agg(F.max("event_ts").alias("max_ts"))
    recent = alerts.crossJoin(horizon).where(
        F.col("event_ts") > F.col("max_ts") - F.expr(f"INTERVAL {int(lookback_hours)} HOURS")
    )
    return recent.groupBy("site_id").agg(
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("generated_at"),
        F.count(F.lit(1)).alias("alert_count"),
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("severity") == "warning", 1).otherwise(0)).alias("warning_count"),
        F.countDistinct("turbine_id").alias("turbines_affected"),
        F.array_sort(F.collect_set("metric")).alias("metrics"),
        F.max("event_ts").alias("latest_alert_ts"),
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    names = Names(catalog=args.catalog)
    digest = build_digest(
        spark.read.table(names.turbine_alerts),
        args.run_id,
        parse_site_ids(args.site_ids),
        args.lookback_hours,
    )
    digest.write.mode("append").saveAsTable(names.alert_digest)
    digest.show(truncate=False)


if __name__ == "__main__":
    main()
