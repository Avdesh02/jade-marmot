import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random
import time

spark = SparkSession.builder \
    .appName("WindTurbineStreamingPipeline") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/wind_turbine/stream_checkpoints") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseMode", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.timestampRebaseModeToUTC", "TRUE")

# Read static turbine info
with open("../static/turbine_info.json") as f:
    turbine_data = json.load(f)

turbine_ids = [t["turbine_id"] for t in turbine_data["turbines"]]

# Create dimension tables if not exist
dim_turbine = spark.table("global_temp.dim_turbine") if spark.catalog.tableExists("global_temp.dim_turbine") else \
    spark.createDataFrame(turbine_data["turbines"], [
        StructField("turbine_id", StringType(), False),
        StructField("location", StringType(), False),
        StructField("capacity_mw", DoubleType(), False),
        StructField("commission_date", DateType(), False),
        StructField("status", StringType(), False),
        StructField("hub_height_m", DoubleType(), False),
        StructField("rotor_diameter_m", DoubleType(), False)
    ])

dim_time = spark.table("global_temp.dim_time") if spark.catalog.tableExists("global_temp.dim_time") else \
    spark.range(100).withColumn("id", F.monotonically_increasing_id()) \
    .withColumn("date", current_date()) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("weekday", dayofweek("date")) \
    .withColumn("quarter", quarter("date"))

# Define sensor tag schema (200 tags)
sensor_tags = [f"tag_{i:03d}" for i in range(1, 201)]

iot_schema = StructType([
    StructField("turbine_id", StringType(), False),
    StructField("reading_timestamp", TimestampType(), False),
    StructField("wind_speed_mps", DoubleType(), True),
    StructField("power_output_kw", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    *[StructField(tag, DoubleType(), True) for tag in sensor_tags]
])

# 1. Simulate streaming source from socket or file stream
# In production: spark.readStream.format("kafka") or format("socket")
# Here we create a rate stream and transform it to IoT data

# Create a base stream with one record per second per turbine
rate_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", len(turbine_ids)) \
    .load() \
    .withColumn("turbine_id", expr("""case when id % 5 = 0 then 'TURB-001'
                                                          when id % 5 = 1 then 'TURB-002'
                                                          when id % 5 = 2 then 'TURB-003'
                                                          when id % 5 = 4 then 'TURB-004'
                                                          else 'TURB-005' end""")) \
    .withColumn("reading_timestamp", to_timestamp(col("timestamp")))

# 2. Generate sensor values and all 200 tag values
def generate_sensor_values(row):
    turbine_id = row["turbine_id"]
    timestamp = row["reading_timestamp"]
    
    # Base values depending on turbine and time
    base_wind_speed = 8.0 + 2.0 * sin(timestamp.cast("double") / 3600000.0)
    power_kw = random.uniform(0.5, 2500.0)
    rpm_val = random.uniform(8, 22) * 60
    temp = random.uniform(25, 45)
    pressure = random.uniform(980, 1030)
    
    tag_vals = {tag: round(random.uniform(0, 100), 2) for tag in sensor_tags}
    
    return (turbine_id, timestamp, base_wind_speed, power_kw, rpm_val, temp, pressure, 
            *[tag_vals[tag] for tag in sensor_tags])

# UDF to generate full IoT row
generate_iot_udf = udf(generate_sensor_values, iot_schema)

# Transform the stream
iot_stream = rate_stream \
    .withColumn("iot_data", generate_iot_udf(col("turbine_id"), col("reading_timestamp"))) \
    .select(
        col("turbine_id"),
        col("iot_data.reading_timestamp"),
        col("iot_data.wind_speed_mps"),
        col("iot_data.power_output_kw"),
        col("iot_data.rpm"),
        col("iot_data.temperature_c"),
        col("iot_data.pressure_hpa"),
        *[col(f"iot_data.{tag}").alias(tag) for tag in sensor_tags]
    )

# 3. Add time dimensions for joining
iot_enriched = iot_stream \
    .withColumn("date_utc", to_date("reading_timestamp")) \
    .withColumn("hour", hour("reading_timestamp")) \
    .withColumn("minute", minute("reading_timestamp"))

# 4. Join with dimension tables for enrichment
dim_turbine_joined = iot_enriched.join(
    dim_turbine,
    iot_enriched.turbine_id == dim_turbine.turbine_id,
    "inner"
)

dim_time_joined = dim_turbine_joined.join(
    dim_time,
    (dim_turbine_joined.date_utc == dim_time.date) & 
    (dim_turbine_joined.hour == dim_time.hour),
    "inner"
).drop(dim_time.turbine_id, dim_time.date, dim_time.year, dim_time.month, dim_time.day, dim_time.weekday, dim_time.quarter)

# 5. Write streaming output to Unity Catalog Delta table
# Using foreachBatch for delta Lake writing
def write_to_delta(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("wind_turbine.streaming_sensor_readings")

# Start the streaming query
query = dim_turbine_joined.writeStream \
    .foreachBatch(write_to_delta) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"Streaming pipeline started. Processing rate stream with {len(turbine_ids)} turbines")
print(f"Query ID: {query.id}")
print("Writing to table: wind_turbine.streaming_sensor_readings")

# Await termination with a timeout for demo purposes
try:
    query.awaitTermination(60)  # 60 seconds for demo
    print("Streaming completed after 60 seconds")
except Exception as e:
    print(f"Streaming stopped: {e}")
    query.stop()

spark.stop()