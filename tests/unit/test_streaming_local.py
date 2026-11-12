"""End-to-end over real files: simulator -> landing JSON -> streaming bronze -> silver.

Uses the plain file source in place of Auto Loader (``use_autoloader=False``) and a
memory sink, so it exercises the same read/parse/rule/dedupe code the pipeline runs.
"""

import sys

import pytest

from conftest import FIXED_START
from jade_marmot_wind.simulator import simulate_site, write_landing_files
from jade_marmot_wind.transforms import (
    READING_RULES,
    SENSOR_PAYLOAD_SCHEMA,
    dedupe_readings,
    failing_rows,
    parse_sensor_readings,
    read_landing,
    silver_readings,
    with_rule_results,
)

pytestmark = [
    pytest.mark.local_only,
    # File-source streaming needs Hadoop's native Windows libraries (winutils/hadoop.dll)
    # matching Spark's Hadoop version; CI runs this on Linux.
    pytest.mark.skipif(sys.platform == "win32", reason="needs Hadoop native libs on Windows"),
]


def _drain(df, name, tmp_path):
    q = (
        df.writeStream.format("memory")
        .queryName(name)
        .option("checkpointLocation", str(tmp_path / f"chk_{name}"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return df.sparkSession.table(name)


def test_landing_to_silver_with_rerun(spark, site, tmp_path):
    landing = tmp_path / "landing"
    result = simulate_site(site, start=FIXED_START, minutes=60, fault_rate=0.1, seed="e2e")
    write_landing_files(result, landing, site.site_id, FIXED_START, "run1")
    # A rerun of the same window lands the same reading ids again.
    write_landing_files(result, landing, site.site_id, FIXED_START, "run2")

    bronze = read_landing(
        spark, str(landing / "sensors" / site.site_id), SENSOR_PAYLOAD_SCHEMA, use_autoloader=False
    )
    checked = with_rule_results(parse_sensor_readings(bronze), READING_RULES)

    silver = _drain(dedupe_readings(silver_readings(checked), "1 day"), "silver_e2e", tmp_path)
    quarantine = _drain(failing_rows(checked), "quarantine_e2e", tmp_path)

    assert silver.count() == result.clean_reading_count
    assert silver.select("reading_id").distinct().count() == result.clean_reading_count
    quarantined = sum(n for k, n in result.faults.items() if k != "duplicate")
    assert quarantine.count() == 2 * quarantined  # once per landed copy
    assert (
        silver.where("_source_file NOT LIKE '%run1_%' AND _source_file NOT LIKE '%run2_%'").count()
        == 0
    )
