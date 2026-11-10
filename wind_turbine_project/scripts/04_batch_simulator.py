#!/usr/bin/env python3
"""
Wind Turbine Batch Simulator
Generates synthetic IoT sensor data for batch processing
Output: Parquet files partitioned by date and turbine ID
"""

import json
import random
import os
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def generate_batch_simulator():
    spark = SparkSession.builder \
        .appName("WindTurbineBatchSimulator") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Read static turbine info
    with open("../static/turbine_info.json") as f:
        turbine_data = json.load(f)

    turbine_ids = [t["turbine_id"] for t in turbine_data["turbines"]]
    locations = [t["location"] for t in turbine_data["turbines"]]
    
    # Generate 60 days of historical data
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=60)
    
    rows = []
    base_date = start_date
    
    for day_offset in range((end_date - start_date).days + 1):
        current_date = base_date + timedelta(days=day_offset)
        
        for t in turbine_data["turbines"]:
            turbine_id = t["turbine_id"]
            location = t["location"]
            capacity = t["capacity_mw"]
            
            # Daily pattern: wind is higher during day, lower at night
            # Seasonal factor based on location
            location_factor = {
                "Rajasthan": 1.3,
                "Tamil Nadu": 1.1,
                "Maharashtra": 0.9,
                "Gujarat": 1.2,
                "Karnataka": 1.0
            }.get(location, 1.0)
            
            for hour in range(24):
                # Diurnal pattern - higher wind during day
                diurnal_factor = max(0.3, 0.5 + 0.7 * sin((hour - 6) * 3.14159 / 12))
                
                # Wind speed varies by time of day and location
                wind_speed = round(random.gauss(base_wind_speed := 8.0 * location_factor * diurnal_factor, 3), 2)
                wind_speed = max(0, min(40, wind_speed))  # Clamp between 0-40 mps
                
                # Power output (curated at capacity during good wind, zero at low wind)
                cut_in_speed = 3.0
                cut_out_speed = 25.0
                rated_speed = 12.0
                
                if wind_speed < cut_in_speed:
                    power_output = 0.0
                elif wind_speed >= cut_out_speed:
                    power_output = 0.0
                else:
                    # Power curve: P = P_rated * ((V - V_ci) / (V_rated - V_ci))^3
                    power_output = round(capacity * 1000 * ((wind_speed - cut_in_speed) / (rated_speed - cut_in_speed))**3, 2)
                
                # RPM (varies with wind speed and turbine design)
                rpm = round(random.uniform(8, 22) * 60 * (wind_speed / 12.0), 2)
                
                # Temperature (varies by location)
                temp_base = { "Rajasthan": 42, "Tamil Nadu": 35, "Maharashtra": 33, "Gujarat": 38, "Karnataka": 30 }
                temperature = round(temp_base.get(location, 30) + random.gauss(0, 5), 2)
                
                # Pressure
                pressure = round(random.gauss(1013, 20), 2)
                
                # Generate all 200 sensor tags with related values
                tag_values = {}
                for i in range(1, 201):
                    # Tags correlated with operating conditions
                    tag_key = f"tag_{i:03d}"
                    # Some tags are environmental, some are mechanical
                    tag_correlation = random.random()
                    if tag_correlation < 0.4:
                        # Environmental tags
                        tag_values[tag_key] = round(random.uniform(0, 100), 2)
                    elif tag_correlation < 0.7:
                        # Mechanical tags correlated with rpm
                        tag_values[tag_key] = round(random.uniform(0, 100), 2)
                    else:
                        # Miscellaneous
                        tag_values[tag_key] = round(random.uniform(0, 100), 2)
                
                row = (
                    str(uuid.uuid4()),
                    turbine_id,
                    location,
                    current_date,
                    hour,
                    wind_speed,
                    power_output,
                    rpm,
                    temperature,
                    pressure,
                    *[
                        tag_values[f"tag_{i:03d}"] for i in range(1, 201)
                    ],
                    datetime.utcnow()
                )
                rows.append(row)
    
    # Create DataFrame with proper schema
    schema = StructType([
        StructField("reading_id", StringType(), False),
        StructField("turbine_id", StringType(), False),
        StructField("location", StringType(), False),
        StructField("date_utc", DateType(), False),
        StructField("hour", IntegerType(), False),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("power_output_kw", DoubleType(), True),
        StructField("rpm", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        *[StructField(f"tag_{i:03d}", DoubleType(), True) for i in range(1, 201)],
        StructField("ingested_at", TimestampType(), False)
    ])
    
    fact_df = spark.createDataFrame(rows, schema)
    
    # Write to Delta Lake in Unity Catalog path
    output_path = "/tmp/wind_turbine/batch_data"
    os.makedirs(output_path, exist_ok=True)
    
    fact_df.write \
        .mode("overwrite") \
        .partitionBy("turbine_id", "date_utc") \
        .format("delta") \
        .save(output_path)
    
    print(f"Batch simulator generated {fact_df.count()} records")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Output path: {output_path}")
    print(f"Turbines: {', '.join(turbine_ids)}")
    
    # Also write as JSON for backup
    fact_df.coalesce(1).write.mode("overwrite").json(f"{output_path}_backup")
    
    spark.stop()

if __name__ == "__main__":
    generate_batch_simulator()