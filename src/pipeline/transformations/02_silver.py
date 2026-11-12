"""Silver: validated readings, a quarantine table, and the turbine dimension via CDC.

Readings: each bronze row is checked against READING_RULES once (in a temporary view);
passing rows are de-duplicated on reading_id into `sensor_readings`, failing rows land in
`sensor_readings_quarantine` with the names of the rules they broke. Doing the split by
hand, rather than with expect_or_drop, keeps the rejected rows instead of discarding them.

Turbine master data: AUTO CDC (the change data flow API) maintains two targets from the
same change feed -- `dim_turbine` as SCD Type 2 for as-of joins, and `turbine_current` as
SCD Type 1 for "what is it now" lookups.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

from jade_marmot_wind.pipeline_config import PipelineConfig
from jade_marmot_wind.transforms import (
    CDC_CONTROL_COLUMNS,
    READING_RULES,
    TURBINE_CDC_RULES,
    dedupe_readings,
    dim_site,
    failing_rows,
    parse_sensor_readings,
    silver_readings,
    standardise_turbine_cdc,
    with_rule_results,
)

cfg = PipelineConfig.from_spark(spark)
names = cfg.names


# --- Sensor readings --------------------------------------------------------------------


@dp.temporary_view(name="sensor_readings_checked")
def sensor_readings_checked():
    return with_rule_results(
        parse_sensor_readings(spark.readStream.table(names.bronze_sensor_readings)),
        READING_RULES,
    )


@dp.table(
    name=names.silver_sensor_readings,
    comment="Validated, de-duplicated sensor readings. One row per reading_id.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["site_id", "event_date"],
)
def sensor_readings():
    checked = spark.readStream.table("sensor_readings_checked")
    return dedupe_readings(silver_readings(checked), cfg.dedupe_watermark)


@dp.table(
    name=names.silver_quarantine,
    comment="Readings that failed at least one quality rule; _failed_rules names them.",
    table_properties={"quality": "silver"},
)
def sensor_readings_quarantine():
    return failing_rows(spark.readStream.table("sensor_readings_checked"))


# --- Turbine master data (change data capture) ------------------------------------------


@dp.temporary_view(name="turbine_cdc_clean")
@dp.expect_all_or_drop(TURBINE_CDC_RULES)
def turbine_cdc_clean():
    return standardise_turbine_cdc(spark.readStream.table(names.bronze_turbine_cdc))


dp.create_streaming_table(
    name=names.dim_turbine,
    comment="Turbine master data, SCD Type 2. __START_AT/__END_AT bound each version; "
    "__END_AT is NULL on the current row.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    name="turbine_scd2_flow",
    target=names.dim_turbine,
    source="turbine_cdc_clean",
    keys=["turbine_id"],
    # __START_AT/__END_AT inherit this column's type, so a TIMESTAMP keeps them directly
    # comparable to event_ts in the gold as-of join.
    sequence_by=col("changed_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=list(CDC_CONTROL_COLUMNS),
    stored_as_scd_type=2,
)

dp.create_streaming_table(
    name=names.turbine_current,
    comment="Turbine master data, SCD Type 1: the latest version of each turbine.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    name="turbine_scd1_flow",
    target=names.turbine_current,
    source="turbine_cdc_clean",
    keys=["turbine_id"],
    sequence_by=col("changed_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=list(CDC_CONTROL_COLUMNS),
    stored_as_scd_type=1,
)


# --- Site dimension (static reference data) ---------------------------------------------


@dp.materialized_view(
    name=names.dim_site,
    comment="Wind sites from the bundled site config; is_enabled marks the sites this "
    "deployment target processes.",
    table_properties={"quality": "silver"},
)
def dim_site_mv():
    return dim_site(spark, cfg.all_sites.values(), cfg.enabled_site_ids)
