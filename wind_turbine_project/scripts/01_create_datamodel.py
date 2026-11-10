import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("WindTurbineDataModel") \
    .getOrCreate()

# Read static turbine info
with open("../static/turbine_info.json") as f:
    turbine_data = json.load(f)

# Create dimensions

# 1. DimTurbine - turbine static information
dim_turbine = spark.createDataFrame(turbine_data["turbines"], [
    StructField("turbine_id", StringType(), False),
    StructField("location", StringType(), False),
    StructField("capacity_mw", DoubleType(), False),
    StructField("commission_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("hub_height_m", DoubleType(), False),
    StructField("rotor_diameter_m", DoubleType(), False)
])

dim_turbine.createOrReplaceGlobalTempView("dim_turbine")

# 2. DimLocation - location hierarchy
dim_location = dim_turbine.select(
    col("location").alias("location_name"),
    col("turbine_id")
).distinct()

dim_location.createOrReplaceGlobalTempView("dim_location")

# 3. DimTime - time dimension (for partitioning)
# We'll generate a range of dates
from pyspark.sql import functions as F
import datetime

start_date = datetime.date(2022, 1, 1)
end_date = datetime.date(2026, 12, 31)
date_range = spark.range((end_date - start_date).days + 1) \
    .withColumn("id", F.monotonically_increasing_id()) \
    .withColumn("date", F.date_add(F.lit(start_date), F.col("id"))) \
    .withColumn("year", F.year("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("day", F.dayofmonth("date")) \
    .withColumn("weekday", F.dayofweek("date")) \
    .withColumn("quarter", F.quarter("date"))

dim_time = date_range.select(
    col("date"),
    col("year"),
    col("month"),
    col("day"),
    col("weekday"),
    col("quarter")
)

dim_time.createOrReplaceGlobalTempView("dim_time")

# 4. FactTable - sensor readings (IoT data)
# Schema for 200+ IoT sensor tags
sensor_tags = [f"tag_{i:03d}" for i in range(1, 201)]

fact_schema = StructType([
    StructField("reading_id", StringType(), False),
    StructField("turbine_id", StringType(), False),
    StructField("date_utc", DateType(), False),
    StructField("hour", IntegerType(), False),
    StructField("minute", IntegerType(), False),
    *[StructField(tag, DoubleType(), True) for tag in sensor_tags],
    StructField("wind_speed_mps", DoubleType(), True),
    StructField("power_output_kw", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    StructField("created_at", TimestampType(), False)
])

# Create sample fact data for demonstration
# In production, this would come from IoT stream/batch
import uuid
import random

rows = []
base_date = datetime.date(2024, 1, 1)

for t in turbine_data["turbines"]:
    turbine_id = t["turbine_id"]
    for day_offset in range(30):  # 30 days of data
        current_date = base_date + datetime.timedelta(days=day_offset)
        for hour in range(24):
            for minute in [0, 15, 30, 45]:  # 4 readings per hour
                reading_id = f"READING_{turbine_id}_{current_date}_{hour:02d}{minute:02d}"
                # Generate realistic sensor values
                base_wind_speed = random.uniform(3, 15)
                power_output = random.uniform(0.5, t["capacity_mw"] * 1000)
                rpm = random.uniform(8, 22) * 60
                temp = random.uniform(25, 45)
                pressure = random.uniform(980, 1030)

                tag_values = {
                    tag: round(random.uniform(0, 100), 2) for tag in sensor_tags
                }

                row = (
                    reading_id,
                    turbine_id,
                    current_date,
                    hour,
                    minute,
                    *[
                        tag_values[tag] for tag in sensor_tags
                    ],
                    round(base_wind_speed, 2),
                    round(power_output, 2),
                    round(rpm, 2),
                    round(temp, 2),
                    round(pressure, 2),
                    datetime.datetime.combine(current_date, datetime.time(hour, minute))
                )
                rows.append(row)

fact_table = spark.createDataFrame(rows, fact_schema)
fact_table.createOrReplaceGlobalTempView("fact_table")

print(f"Data model created successfully!")
print(f"DimTurbine count: {dim_turbine.count()}")
print(f"DimLocation count: {dim_location.count()}")
print(f"DimTime count: {dim_time.count()}")
print(f"FactTable count: {fact_table.count()}")

spark.stop()