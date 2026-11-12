"""Bronze: raw landing files -> streaming tables, one APPEND FLOW per site.

Each enabled site gets its own Auto Loader flow into a shared streaming table. Because
every flow keeps its own checkpoint, enabling a new site (the static `wind.sites`
parameter) just adds a flow -- the existing sites keep their progress and nothing needs a
full refresh. Removing a site stops its flow; rows already landed stay.
"""

from pyspark import pipelines as dp

from jade_marmot_wind.pipeline_config import PipelineConfig
from jade_marmot_wind.transforms import (
    BRONZE_READING_RULES,
    SENSOR_PAYLOAD_SCHEMA,
    TURBINE_CDC_SCHEMA,
    read_landing,
)

cfg = PipelineConfig.from_spark(spark)
names = cfg.names

dp.create_streaming_table(
    name=names.bronze_sensor_readings,
    comment="Raw SCADA sensor readings as landed, one append flow per site. "
    "Event timestamps are kept as strings; silver parses and validates them.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    # Warn-only: bronze keeps every record. The counts show up per rule in the event log.
    expect_all=BRONZE_READING_RULES,
)

dp.create_streaming_table(
    name=names.bronze_turbine_cdc,
    comment="Raw turbine master-data change events as landed, one append flow per site. "
    "Feeds the AUTO CDC flows in silver.",
    table_properties={"quality": "bronze"},
)


def _register_site_flows(site_id: str) -> None:
    # A factory function, not a bare loop body: the decorated functions must close over
    # this call's site_id, not the loop variable's final value.
    sensor_path = cfg.sensor_path(site_id)
    cdc_path = cfg.cdc_path(site_id)

    @dp.append_flow(target=names.bronze_sensor_readings, name=f"ingest_sensors_{site_id.lower()}")
    def ingest_sensors():
        return read_landing(spark, sensor_path, SENSOR_PAYLOAD_SCHEMA)

    @dp.append_flow(target=names.bronze_turbine_cdc, name=f"ingest_turbine_cdc_{site_id.lower()}")
    def ingest_turbine_cdc():
        return read_landing(spark, cdc_path, TURBINE_CDC_SCHEMA)


for _site in cfg.enabled_sites:
    _register_site_flows(_site.site_id)
