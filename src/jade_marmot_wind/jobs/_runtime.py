"""Small helpers shared by the job entry points."""

from __future__ import annotations

import json
import os
from pathlib import Path


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def set_task_value(key: str, value) -> None:
    """Publish a task value for downstream tasks (``{{tasks.<task>.values.<key>}}``).
    Outside Databricks it's just printed, so the entry points run locally too."""
    if on_databricks():
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key=key, value=value)
    print(f"task value {key} = {json.dumps(value)}")


def str_to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return value.strip().lower() in ("1", "true", "yes", "y")


def manifest_dir(landing_root: str, run_id: str) -> Path:
    # Outside the sensors/ and turbine_cdc/ trees, so Auto Loader never picks it up.
    return Path(landing_root) / "_manifests" / run_id
