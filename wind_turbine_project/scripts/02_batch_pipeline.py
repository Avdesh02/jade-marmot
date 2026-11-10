import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("WindTurbineBatchPipeline") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseMode", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.timestampRebaseModeToUTC", "TRUE")

# Read static turbine info
with open("../static/turbine_info.json") as f:
    turbine_data = json.load(f)

turbine_ids = [t["turbine_id"] for t in turbine_data["turbines"]]

# 1. Read and process dimension tables
dim_turbine = spark.table("global_temp.dim_turbine")
dim_time = spark.table("global_temp.dim_time")
dim_location = spark.table("global_temp.dim_location")

# 2. Read fact table from batch source (simulated IoT data)
# In production, this reads from S3, ADLS, or Kafka
fact_batch = spark.table("global_temp.fact_table")

# 3. Filter fact data for last 30 days only
recent_cutoff = (datetime.now().date() - timedelta(days=30)).isoformat()
fact_recent = fact_batch.filter(col("date_utc") >= recent_cutoff)

# 4. Join with dimensions for enriched analytics
enriched_fact = fact_recent.join(
    dim_turbine,
    fact_recent.turbine_id == dim_turbine.turbine_id,
    "inner"
).join(
    dim_time,
    (fact_recent.date_utc == dim_time.date) & 
    (fact_recent.hour == dim_time.hour_approx),
    "inner"
).join(
    dim_location,
    fact_recent.location == dim_location.location_name,
    "inner"
)

# 5. Aggregate metrics per turbine per time bucket
turbine_daily_stats = enriched_fact.groupBy(
    "turbine_id",
    "location",
    "capacity_mw",
    "status",
    "date_utc",
    "year",
    "month"
).agg(
    avg("wind_speed_mps").alias("avg_wind_speed_mps"),
    max("power_output_kw").alias("max_power_kw"),
    avg("power_output_kw").alias("avg_power_kw"),
    avg("rpm").alias("avg_rpm"),
    avg("temperature_c").alias("avg_temp_c"),
    count("reading_id").alias("total_readings"),
    countDistinct("reading_id").alias("unique_readings")
)

# 6. Write enriched data to Unity Catalog Delta table
# In production, use proper path: spark_catalog.database.table_name
turbine_daily_stats.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("wind_turbine.enriched_daily_stats")

# 7. Write aggregated metrics to dimension-friendly fact table
daily_agg = enriched_fact.groupBy(
    "turbine_id",
    "location",
    "date_utc",
    "year",
    "month"
).agg(
    avg("wind_speed_mps").alias("avg_wind_speed"),
    avg("power_output_kw").alias("avg_power_kw"),
    avg("temperature_c").alias("avg_temperature"),
    avg("pressure_hpa").alias("avg_pressure"),
    max("power_output_kw").alias("peak_power_kw"),
    min("wind_speed_mps").alias("min_wind_speed"),
    max("rpm").alias("max_rpm")
)

daily_agg.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("wind_turbine.daily_turbine_metrics")

print("Batch pipeline completed successfully!")
print(f"Processed {fact_recent.count()} recent readings")
print(f"Enriched stats written: {turbine_daily_stats.count()} rows")
print(f"Daily metrics written: {daily_agg.count()} rows")

spark.stop()