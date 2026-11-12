"""DataFrame transformations used by the SDP pipeline.

The pipeline source files under ``src/pipeline/transformations`` are thin: they wire
these functions to SDP decorators. Keeping the logic here means it runs under plain PySpark
in unit tests, without the pipelines runtime.

Everything is written to be safe under ANSI mode (the default on Databricks serverless):
``try_to_timestamp`` / ``try_divide`` instead of casts and ``/`` that would raise.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from jade_marmot_wind.sites import Site

# --- Landing payload schemas -----------------------------------------------------------
# Timestamps stay STRING in bronze: a malformed value is kept verbatim for the quarantine
# table instead of silently becoming NULL at read time.

SENSOR_PAYLOAD_SCHEMA = StructType(
    [
        StructField("reading_id", StringType()),
        StructField("site_id", StringType()),
        StructField("turbine_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("sample_interval_s", IntegerType()),
        StructField("wind_speed_mps", DoubleType()),
        StructField("wind_direction_deg", DoubleType()),
        StructField("power_output_kw", DoubleType()),
        StructField("rotor_rpm", DoubleType()),
        StructField("pitch_angle_deg", DoubleType()),
        StructField("ambient_temp_c", DoubleType()),
        StructField("nacelle_temp_c", DoubleType()),
        StructField("gearbox_oil_temp_c", DoubleType()),
        StructField("generator_temp_c", DoubleType()),
        StructField("pressure_hpa", DoubleType()),
        StructField("vibration_mm_s", DoubleType()),
        StructField("status_code", StringType()),
        StructField("tags", MapType(StringType(), DoubleType())),
        StructField("scenario", StringType()),
        StructField("sim_run_id", StringType()),
    ]
)

TURBINE_CDC_SCHEMA = StructType(
    [
        StructField("turbine_id", StringType()),
        StructField("site_id", StringType()),
        StructField("model", StringType()),
        StructField("capacity_mw", DoubleType()),
        StructField("hub_height_m", DoubleType()),
        StructField("rotor_diameter_m", DoubleType()),
        StructField("commission_date", StringType()),
        StructField("status", StringType()),
        StructField("operation", StringType()),
        StructField("change_seq", LongType()),
        StructField("changed_at", StringType()),
    ]
)

# Columns that drive AUTO CDC but don't belong in the dimension itself.
CDC_CONTROL_COLUMNS = ("operation", "change_seq", "_rescued_data", "_source_file", "_ingested_at")

# --- Quality rules ----------------------------------------------------------------------
# Every rule is NULL-safe (evaluates to TRUE/FALSE, never NULL): a NULL from a rule would
# count as a pass in `with_rule_results`, letting bad rows through.

READING_RULES: dict[str, str] = {
    "valid_turbine_id": "turbine_id IS NOT NULL AND turbine_id RLIKE '^[A-Z]{2}[0-9]{2}-WTG[0-9]{2}$'",
    "site_matches_turbine": "site_id IS NOT NULL AND turbine_id IS NOT NULL "
    "AND substring(turbine_id, 1, 4) = site_id",
    "valid_event_ts": "event_ts IS NOT NULL",
    "wind_speed_in_range": "wind_speed_mps IS NOT NULL AND wind_speed_mps BETWEEN 0 AND 60",
    "power_in_range": "power_output_kw IS NOT NULL AND power_output_kw BETWEEN -50 AND 6000",
    "gearbox_temp_plausible": "gearbox_oil_temp_c IS NOT NULL "
    "AND gearbox_oil_temp_c BETWEEN -40 AND 150",
    "rotor_rpm_in_range": "rotor_rpm IS NOT NULL AND rotor_rpm BETWEEN 0 AND 40",
}

# Same rules against the bronze shape, where event_ts is still the raw string. Applied as
# warn-only expectations so the event log reports bad-record counts at ingestion.
BRONZE_READING_RULES: dict[str, str] = {
    **READING_RULES,
    "valid_event_ts": "event_ts IS NOT NULL AND try_to_timestamp(event_ts) IS NOT NULL",
}

TURBINE_CDC_RULES: dict[str, str] = {
    "has_turbine_id": "turbine_id IS NOT NULL",
    "has_site_id": "site_id IS NOT NULL",
    "valid_operation": "operation IN ('UPSERT', 'DELETE')",
    "has_sequence": "changed_at IS NOT NULL",
}

AVAILABLE_STATUSES = ("RUNNING", "IDLE", "CUT_OUT")


# --- Bronze -----------------------------------------------------------------------------


def read_landing(
    spark: SparkSession, path: str, schema: StructType, *, use_autoloader: bool = True
) -> DataFrame:
    """Streaming read of JSON-lines landing files, with source lineage columns.

    ``use_autoloader=False`` swaps Auto Loader for the plain file source so the same code
    path runs in local tests.
    """
    if use_autoloader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("rescuedDataColumn", "_rescued_data")
        )
    else:
        reader = spark.readStream.format("json")
    df = reader.schema(schema).option("pathGlobFilter", "*.json").load(path)
    if not use_autoloader:
        df = df.withColumn("_rescued_data", F.lit(None).cast(StringType()))
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def standardise_turbine_cdc(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(F.trim("turbine_id")).alias("turbine_id"),
        F.upper(F.trim("site_id")).alias("site_id"),
        "model",
        "capacity_mw",
        "hub_height_m",
        "rotor_diameter_m",
        F.expr("try_to_timestamp(commission_date)").cast("date").alias("commission_date"),
        F.lower("status").alias("status"),
        F.upper(F.trim("operation")).alias("operation"),
        "change_seq",
        F.expr("try_to_timestamp(changed_at)").alias("changed_at"),
        "_rescued_data",
        "_source_file",
        "_ingested_at",
    )


# --- Silver -----------------------------------------------------------------------------


def parse_sensor_readings(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumnRenamed("event_ts", "event_ts_raw")
        .withColumn("event_ts", F.expr("try_to_timestamp(event_ts_raw)"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("site_id", F.upper(F.trim("site_id")))
        .withColumn("turbine_id", F.upper(F.trim("turbine_id")))
        .withColumn("status_code", F.upper("status_code"))
    )


def with_rule_results(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Adds ``_failed_rules``: names of the rules a row breaks (empty array if none)."""
    checks = [
        F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()
    ]
    return df.withColumn("_failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def passing_rows(df: DataFrame) -> DataFrame:
    return df.where(F.size("_failed_rules") == 0).drop("_failed_rules")


def failing_rows(df: DataFrame) -> DataFrame:
    return df.where(F.size("_failed_rules") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )


def dedupe_readings(readings: DataFrame, watermark: str) -> DataFrame:
    """Drops repeat ``reading_id``s (gateway retries, simulator reruns) within the
    watermark. Streaming-only: batch DataFrames should use ``dropDuplicates``."""
    return readings.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(
        ["reading_id"]
    )


SILVER_READING_COLUMNS = (
    "reading_id",
    "site_id",
    "turbine_id",
    "event_ts",
    "event_date",
    "sample_interval_s",
    "wind_speed_mps",
    "wind_direction_deg",
    "power_output_kw",
    "rotor_rpm",
    "pitch_angle_deg",
    "ambient_temp_c",
    "nacelle_temp_c",
    "gearbox_oil_temp_c",
    "generator_temp_c",
    "pressure_hpa",
    "vibration_mm_s",
    "status_code",
    "tags",
    "scenario",
    "sim_run_id",
    "_source_file",
    "_ingested_at",
)


def silver_readings(checked: DataFrame) -> DataFrame:
    return passing_rows(checked).select(*SILVER_READING_COLUMNS)


def dim_site(spark: SparkSession, sites: Iterable[Site], enabled_ids: Iterable[str]) -> DataFrame:
    enabled = set(enabled_ids)
    schema = StructType(
        [
            StructField("site_id", StringType(), False),
            StructField("site_name", StringType()),
            StructField("state", StringType()),
            StructField("timezone", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("wind_factor", DoubleType()),
            StructField("ambient_temp_c", DoubleType()),
            StructField("turbine_count", IntegerType()),
            StructField("capacity_mw", DoubleType()),
            StructField("is_enabled", BooleanType()),
        ]
    )
    rows = [{**s.to_row(), "is_enabled": s.site_id in enabled} for s in sites]
    return spark.createDataFrame([tuple(r[f.name] for f in schema) for r in rows], schema)


# --- Gold -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Threshold:
    metric: str
    warning: float
    critical: float


DEFAULT_THRESHOLDS = (
    Threshold("wind_speed_mps", 20.0, 25.0),
    Threshold("gearbox_oil_temp_c", 80.0, 95.0),
    Threshold("generator_temp_c", 110.0, 130.0),
    Threshold("vibration_mm_s", 7.1, 11.0),
)


def thresholds_from_conf(get: Callable[[str, str], str]) -> tuple[Threshold, ...]:
    """Thresholds from pipeline configuration, ``wind.threshold.<metric>.<level>``."""
    out = []
    for d in DEFAULT_THRESHOLDS:
        warning = float(get(f"wind.threshold.{d.metric}.warning", str(d.warning)))
        critical = float(get(f"wind.threshold.{d.metric}.critical", str(d.critical)))
        if warning > critical:
            raise ValueError(f"{d.metric}: warning {warning} is above critical {critical}")
        out.append(Threshold(d.metric, warning, critical))
    return tuple(out)


def _severity(t: Threshold) -> Column:
    v = F.col(t.metric)
    return F.when(v >= t.critical, F.lit("critical")).when(v >= t.warning, F.lit("warning"))


def turbine_alerts(readings: DataFrame, thresholds: Iterable[Threshold]) -> DataFrame:
    """One row per (reading, breached metric). Critical wins over warning per metric."""
    candidates = [
        F.struct(
            F.lit(t.metric).alias("metric"),
            _severity(t).alias("severity"),
            F.col(t.metric).cast("double").alias("value"),
            F.when(_severity(t) == "critical", F.lit(t.critical))
            .otherwise(F.lit(t.warning))
            .alias("threshold"),
        )
        for t in thresholds
    ]
    breaches = F.filter(F.array(*candidates), lambda a: a["severity"].isNotNull())
    return readings.select(
        "reading_id",
        "site_id",
        "turbine_id",
        "event_ts",
        "status_code",
        F.explode(breaches).alias("a"),
    ).select(  # fmt: skip
        F.concat_ws(":", "reading_id", "a.metric").alias("alert_id"),
        "reading_id",
        "site_id",
        "turbine_id",
        "event_ts",
        "status_code",
        F.col("a.metric").alias("metric"),
        F.col("a.severity").alias("severity"),
        F.col("a.value").alias("value"),
        F.col("a.threshold").alias("threshold"),
    )


def hourly_metrics(readings: DataFrame, dim_turbine_scd2: DataFrame) -> DataFrame:
    """Per-turbine hourly aggregates, joined as-of to the SCD2 dimension so an hour is
    rated against the capacity in force at the time, not today's."""
    interval = F.coalesce(F.col("sample_interval_s"), F.lit(60))
    agg = (
        readings.withColumn("hour_start", F.date_trunc("hour", "event_ts"))
        .groupBy("site_id", "turbine_id", "hour_start")
        .agg(
            F.count(F.lit(1)).alias("reading_count"),
            F.avg("wind_speed_mps").alias("avg_wind_speed_mps"),
            F.max("wind_speed_mps").alias("max_wind_speed_mps"),
            F.avg("power_output_kw").alias("avg_power_kw"),
            F.max("power_output_kw").alias("max_power_kw"),
            (F.sum(F.col("power_output_kw") * interval) / 3600.0).alias("energy_kwh"),
            (F.sum(interval) / 3600.0).alias("covered_hours"),
            F.avg("gearbox_oil_temp_c").alias("avg_gearbox_oil_temp_c"),
            F.max("gearbox_oil_temp_c").alias("max_gearbox_oil_temp_c"),
            F.max("vibration_mm_s").alias("max_vibration_mm_s"),
            F.avg(F.when(F.col("status_code").isin(*AVAILABLE_STATUSES), 1.0).otherwise(0.0)).alias(
                "availability"
            ),
        )
    ).alias("m")

    d = dim_turbine_scd2.select(
        "turbine_id", "model", "capacity_mw", "__START_AT", "__END_AT"
    ).alias("d")
    joined = agg.join(
        d,
        (F.col("m.turbine_id") == F.col("d.turbine_id"))
        & (F.col("m.hour_start") >= F.col("d.__START_AT"))
        & (F.col("d.__END_AT").isNull() | (F.col("m.hour_start") < F.col("d.__END_AT"))),
        "left",
    )
    rated_kwh = F.col("d.capacity_mw") * 1000.0 * F.col("m.covered_hours")
    return joined.select(
        "m.site_id",
        "m.turbine_id",
        "m.hour_start",
        F.to_date("m.hour_start").alias("metric_date"),
        "d.model",
        "d.capacity_mw",
        "m.reading_count",
        F.round("m.avg_wind_speed_mps", 3).alias("avg_wind_speed_mps"),
        F.round("m.max_wind_speed_mps", 3).alias("max_wind_speed_mps"),
        F.round("m.avg_power_kw", 3).alias("avg_power_kw"),
        F.round("m.max_power_kw", 3).alias("max_power_kw"),
        F.round("m.energy_kwh", 3).alias("energy_kwh"),
        F.round("m.covered_hours", 4).alias("covered_hours"),
        F.round(F.try_divide(F.col("m.energy_kwh"), rated_kwh), 4).alias("capacity_factor"),
        F.round("m.availability", 4).alias("availability"),
        F.round("m.avg_gearbox_oil_temp_c", 2).alias("avg_gearbox_oil_temp_c"),
        F.round("m.max_gearbox_oil_temp_c", 2).alias("max_gearbox_oil_temp_c"),
        F.round("m.max_vibration_mm_s", 2).alias("max_vibration_mm_s"),
    )


def site_daily_kpis(hourly: DataFrame, alerts: DataFrame, sites: DataFrame) -> DataFrame:
    rated_kwh = F.col("capacity_mw") * 1000.0 * F.col("covered_hours")
    daily = hourly.groupBy("site_id", "metric_date").agg(
        F.countDistinct("turbine_id").alias("turbines_reporting"),
        F.sum("reading_count").alias("reading_count"),
        F.round(F.sum("energy_kwh") / 1000.0, 4).alias("energy_mwh"),
        F.round(F.try_divide(F.sum("energy_kwh"), F.sum(rated_kwh)), 4).alias("capacity_factor"),
        F.round(F.avg("availability"), 4).alias("availability"),
        F.max("max_wind_speed_mps").alias("max_wind_speed_mps"),
    )
    alert_counts = (
        alerts.withColumn("metric_date", F.to_date("event_ts"))
        .groupBy("site_id", "metric_date")
        .agg(
            F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_alerts"),
            F.sum(F.when(F.col("severity") == "warning", 1).otherwise(0)).alias("warning_alerts"),
        )
    )
    return (
        daily.join(alert_counts, ["site_id", "metric_date"], "left")
        .join(sites.select("site_id", "site_name", "state"), "site_id", "left")
        .select(
            "site_id",
            "site_name",
            "state",
            "metric_date",
            "turbines_reporting",
            "reading_count",
            "energy_mwh",
            "capacity_factor",
            "availability",
            "max_wind_speed_mps",
            F.coalesce("critical_alerts", F.lit(0)).cast("long").alias("critical_alerts"),
            F.coalesce("warning_alerts", F.lit(0)).cast("long").alias("warning_alerts"),
        )
    )


def fleet_status(turbines_current: DataFrame, readings: DataFrame, alerts: DataFrame) -> DataFrame:
    """Latest known state of every turbine: master data (SCD1) + last reading + alerts in
    the 24h before that reading."""
    latest = readings.groupBy("turbine_id").agg(
        F.max("event_ts").alias("last_seen_ts"),
        F.max_by(
            F.struct("status_code", "wind_speed_mps", "power_output_kw", "gearbox_oil_temp_c"),
            "event_ts",
        ).alias("last"),
    )
    recent_alerts = (
        alerts.alias("a")
        .join(latest.alias("l"), "turbine_id")
        .where(F.col("a.event_ts") > F.col("l.last_seen_ts") - F.expr("INTERVAL 24 HOURS"))
        .groupBy("turbine_id")
        .agg(
            F.sum(F.when(F.col("a.severity") == "critical", 1).otherwise(0)).alias("crit_24h"),
            F.sum(F.when(F.col("a.severity") == "warning", 1).otherwise(0)).alias("warn_24h"),
        )
    )
    crit = F.coalesce(F.col("crit_24h"), F.lit(0))
    warn = F.coalesce(F.col("warn_24h"), F.lit(0))
    health = (
        F.when(F.col("status") != "active", F.col("status"))
        .when(F.col("last_seen_ts").isNull(), F.lit("no_data"))
        .when(crit > 0, F.lit("alarm"))
        .when(warn > 0, F.lit("warning"))
        .otherwise(F.lit("ok"))
    )
    return (
        turbines_current.join(latest, "turbine_id", "left")
        .join(recent_alerts, "turbine_id", "left")
        .select(
            "turbine_id",
            "site_id",
            "model",
            "capacity_mw",
            "status",
            "last_seen_ts",
            F.col("last.status_code").alias("last_status_code"),
            F.col("last.wind_speed_mps").alias("last_wind_speed_mps"),
            F.col("last.power_output_kw").alias("last_power_output_kw"),
            F.col("last.gearbox_oil_temp_c").alias("last_gearbox_oil_temp_c"),
            crit.cast("long").alias("critical_alerts_24h"),
            warn.cast("long").alias("warning_alerts_24h"),
            health.alias("health_state"),
        )
    )
