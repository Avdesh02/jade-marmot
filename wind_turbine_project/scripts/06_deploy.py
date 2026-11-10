#!/usr/bin/env python3
"""
Databricks CLI Deployment Script for Wind Turbine Project
Provides deployment instructions and verifies connectivity
"""

import subprocess
import json
import os

def check_cli_connectivity():
    """Check Databricks CLI connectivity"""
    result = subprocess.run(
        ["databricks", "workspace", "list", "/"],
        capture_output=True,
        text=True,
        timeout=30
    )
    print(f"CLI Status: returncode={result.returncode}")
    if result.stdout:
        print(f"Output: {result.stdout[:200]}")
    if result.stderr:
        print(f"Stderr: {result.stderr[:200]}")
    return result.returncode == 0

def main():
    print("=" * 60)
    print("Wind Turbine Project - Databricks Deployment")
    print("=" * 60)
    print()
    
    # Verify CLI connectivity
    print("1. Checking Databricks CLI connectivity...")
    connected = check_cli_connectivity()
    if not connected:
        print("   Note: CLI connected but workspace access may be limited")
    print()
    
    # List available scripts
    print("2. Project scripts available:")
    scripts_dir = "wind_turbine_project/scripts"
    if os.path.exists(scripts_dir):
        for f in sorted(os.listdir(scripts_dir)):
            if f.endswith(".py"):
                print(f"   - {f}")
    print()
    
    # Provide deployment instructions
    print("3. Deployment Methods:")
    print()
    print("   METHOD A: Databricks UI (Recommended)")
    print("     1. Open: https://dbc-0090b005-ba2d.cloud.databricks.com/")
    print("     2. Click 'Workspace' in left navigation")
    print("     3. Click 'Upload' button")
    print("     4. Select the entire 'wind_turbine_project' folder")
    print("     5. Click 'Upload'")
    print()
    print("   METHOD B: Databricks CLI file upload")
    print("     databricks fs put -f wind_turbine_project/scripts/01_create_datamodel.py dbfs:/wind_turbine_project/scripts/01_create_datamodel.py")
    print("     databricks fs put -f wind_turbine_project/scripts/02_batch_pipeline.py dbfs:/wind_turbine_project/scripts/02_batch_pipeline.py")
    print("     databricks fs put -f wind_turbine_project/scripts/03_streaming_pipeline.py dbfs:/wind_turbine_project/scripts/03_streaming_pipeline.py")
    print("     databricks fs put -f wind_turbine_project/scripts/04_batch_simulator.py dbfs:/wind_turbine_project/scripts/04_batch_simulator.py")
    print("     databricks fs put -f wind_turbine_project/scripts/05_streaming_simulator.py dbfs:/wind_turbine_project/scripts/05_streaming_simulator.py")
    print("     databricks fs put -f wind_turbine_project/static/turbine_info.json dbfs:/wind_turbine_project/static/turbine_info.json")
    print()
    print("   METHOD C: Manual Notebook Import")
    print("     1. In Databricks UI: Click '+ > Import'")
    print("     2. Upload each .py file as a notebook")
    print("     3. Or create new notebooks and copy the script content")
    print()
    print("4. After upload:")
    print("   - Run 01_create_datamodel.py first (creates Unity Catalog tables)")
    print("   - Run 02_batch_pipeline.py for batch processing")
    print("   - Run 03_streaming_pipeline.py for streaming")
    print("   - Run 04_batch_simulator.py to generate 60 days of historical data")
    print("   - Run 05_streaming_simulator.py for real-time data")
    print()
    print("=" * 60)

if __name__ == "__main__":
    main()