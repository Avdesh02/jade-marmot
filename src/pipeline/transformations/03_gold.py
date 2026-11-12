"""Gold: threshold alerts (streaming) and three materialized views.

`turbine_alerts` is a streaming table -- an alert is an immutable fact, so it is computed
once per reading as it arrives. The aggregates are materialized views: they're recomputed
(incrementally where the engine can) from silver on every update, so late readings and
SCD2 corrections flow through without any merge logic here.
"""

from pyspark import pipelines as dp

from jade_marmot_wind.pipeline_config import PipelineConfig
from jade_marmot_wind.transforms import (
    fleet_status,
    hourly_metrics,
    site_daily_kpis,
    turbine_alerts,
)

cfg = PipelineConfig.from_spark(spark)
names = cfg.names


@dp.table(
    name=names.turbine_alerts,
    comment="One row per reading per breached threshold. Thresholds come from the "
    "pipeline configuration (wind.threshold.<metric>.<warning|critical>).",
    table_properties={"quality": "gold"},
)
def turbine_alerts_st():
    return turbine_alerts(spark.readStream.table(names.silver_sensor_readings), cfg.thresholds)


@dp.materialized_view(
    name=names.hourly_metrics,
    comment="Hourly per-turbine production and condition metrics, rated against the "
    "turbine's capacity in force at the time (SCD2 as-of join).",
    table_properties={"quality": "gold"},
)
def turbine_hourly_metrics():
    return hourly_metrics(
        spark.read.table(names.silver_sensor_readings), spark.read.table(names.dim_turbine)
    )


@dp.materialized_view(
    name=names.site_daily_kpis,
    comment="Daily site KPIs: energy, capacity factor, availability and alert counts.",
    table_properties={"quality": "gold"},
)
def site_daily_kpis_mv():
    return site_daily_kpis(
        spark.read.table(names.hourly_metrics),
        spark.read.table(names.turbine_alerts),
        spark.read.table(names.dim_site),
    )


@dp.materialized_view(
    name=names.fleet_status,
    comment="Current state of every turbine: master data, last reading, 24h alert counts "
    "and a derived health_state.",
    table_properties={"quality": "gold"},
)
def fleet_status_mv():
    return fleet_status(
        spark.read.table(names.turbine_current),
        spark.read.table(names.silver_sensor_readings),
        spark.read.table(names.turbine_alerts),
    )
