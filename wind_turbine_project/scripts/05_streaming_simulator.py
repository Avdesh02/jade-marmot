#!/usr/bin/env python3
"""
Wind Turbine Streaming Simulator
Generates real-time IoT sensor data stream for structured streaming
Output: Can write to socket, Kafka, or Delta Lake for streaming pipeline consumption
"""

import json
import random
import time
import os
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def generate_streaming_simulator():
    spark = SparkSession.builder \
        .appName("WindTurbineStreamingSimulator") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Read static turbine info
    with open("../static/turbine_info.json") as f:
        turbine_data = json.load(f)

    turbine_ids = [t["turbine_id"] for t in turbine_data["turbines"]]
    locations = [t["location"] for t in turbine_data["turbines"]]
    
    # Sensor tag definitions
    sensor_tags = [f"tag_{i:03d}" for i in range(1, 201)]
    
    # Generate real-time stream records
    rate = 5  # records per second (configurable)
    
    # UDF to generate a single IoT reading
    def generate_iot_reading(turbine_id):
        t = next(t for t in turbine_data["turbines"] if t["turbine_id"] == turbine_id)
        location = t["location"]
        capacity = t["capacity_mw"]
        
        # Current time for diurnal pattern
        now = datetime.utcnow()
        hour = now.hour
        
        # Diurnal wind pattern
        diurnal = max(0.3, 0.5 + 0.7 * sin((hour - 6) * 3.14159 / 12))
        location_factor = {"Rajasthan": 1.3, "Tamil Nadu": 1.1, "Maharashtra": 0.9, 
                          "Gujarat": 1.2, "Karnataka": 1.0}.get(location, 1.0)
        
        # Wind speed with some randomness
        base_wind = 8.0 * location_factor * diurnal
        wind_speed = round(random.gauss(base_wind, 3), 2)
        wind_speed = max(0, min(40, wind_speed))
        
        # Power output using simplified power curve
        cut_in, cut_out, rated = 3.0, 25.0, 12.0
        if wind_speed < cut_in or wind_speed > cut_out:
            power_output = 0.0
        else:
            power_output = round(capacity * 1000 * ((wind_speed - cut_in) / (rated - cut_in))**3, 2)
        
        # RPM
        rpm = round(random.uniform(8, 22) * 60 * (wind_speed / 12.0), 2)
        
        # Temperature and pressure by location
        temp_base = {"Rajasthan": 42, "Tamil Nadu": 35, "Maharashtra": 33, "Gujarat": 38, "Karnataka": 30}
        temperature = round(temp_base.get(location, 30) + random.gauss(0, 3), 2)
        pressure = round(random.gauss(1013, 15), 2)
        
        # 200 sensor tags
        tag_values = {f"tag_{i:03d}": round(random.uniform(0, 100), 2) for i in range(1, 201)}
        
        return (turbine_id, wind_speed, power_output, rpm, temperature, pressure, 
                *[tag_values[tag] for tag in sensor_tags])
    
    # Create schema
    iot_schema = StructType([
        StructField("turbine_id", StringType(), False),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("power_output_kw", DoubleType(), True),
        StructField("rpm", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        *[StructField(tag, DoubleType(), True) for tag in sensor_tags]
    ])
    
    # Generate records in a loop for streaming simulation
    rows = []
    start_time = datetime.utcnow()
    
    print(f"Generating streaming data at {rate} records/second for {len(turbine_ids)} turbines")
    print(f"Duration: 60 seconds (approx {rate * 60 * len(turbine_ids)} total records)")
    
    for second in range(60):  # 60 seconds of simulated data
        for _ in range(rate):
            for turbine_id in turbine_ids:
                values = generate_iot_reading(turbine_id)
                reading_id = str(uuid.uuid4())
                reading_timestamp = (start_time + timedelta(seconds=second)).timestamp()
                
                row = (reading_id,) + (turbine_id,) + \
                      (reading_timestamp,) + values + \
                      (datetime.utcnow(),)
                rows.append(row)
        
        # Small sleep to respect rate
        time.sleep(1.0 / rate / len(turbine_ids) / rate if rate > 0 else 0)
    
    # Create DataFrame
    fact_df = spark.createDataFrame(rows, iot_schema)
    
    # Add date and hour for dimension joining
    fact_df = fact_df.withColumn("date_utc", to_date(from_timestamp(col("ingested_at")))) \
        .withColumn("hour", hour(from_timestamp(col("ingested_at"))))
    
    # Write to Delta Lake for streaming pipeline to read from
    output_path = "/tmp/wind_turbine/streaming_data"
    os.makedirs(output_path, exist_ok=True)
    
    # Write in micro-batch format compatible with structured streaming
    query = fact_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
            .mode("append") \
            .format("delta") \
            .save(f"{output_path}/batch_{batch_id}")) \
        .outputMode("append") \
        .start()
    
    print(f"Streaming simulator generated {fact_df.count()} records")
    print(f"Output path: {output_path}")
    print(f"Turbines: {', '.join(turbine_ids)}")
    print(f"Date range covers hour: {fact_df.select('hour').distinct().collect()}")
    
    # Wait for streaming to complete
    query.awaitTermination()
    
    spark.stop()

if __name__ == "__main__":
    generate_streaming_simulator()