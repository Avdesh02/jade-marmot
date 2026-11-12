"""Static checks on the bundle definition: naming standard and parameter wiring."""

from pathlib import Path

import pytest
import yaml

from jade_marmot_wind.naming import model_name_from_folder

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.local_only


def _load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def bundle():
    root = _load(ROOT / "databricks.yml")
    resources: dict = {}
    for f in sorted((ROOT / "resources").glob("*.yml")):
        for kind, items in (_load(f).get("resources") or {}).items():
            resources.setdefault(kind, {}).update(items)
    return root, resources


def test_model_name_default_matches_git_folder(bundle):
    root, _ = bundle
    assert root["variables"]["model_name"]["default"] == model_name_from_folder(ROOT.name)
    assert root["bundle"]["name"] == model_name_from_folder(ROOT.name)


def test_every_named_object_carries_model_name(bundle):
    _, resources = bundle
    prefix = "${var.model_name}_${bundle.target}_"
    for kind in ("jobs", "pipelines"):
        for key, spec in resources[kind].items():
            assert spec["name"] == prefix + key, f"{kind}.{key}"
            assert spec["tags"]["model_name"] == "${var.model_name}"
    root, _ = bundle
    assert root["variables"]["catalog"]["default"] == "${var.model_name}_${bundle.target}"
    assert root["variables"]["landing_root"]["default"].startswith(
        "/Volumes/${var.model_name}_${bundle.target}/"
    )
    # Every UC object lives in the model-named catalog.
    for kind in ("schemas", "volumes"):
        for key, spec in resources[kind].items():
            assert spec["catalog_name"] == "${var.catalog}", f"{kind}.{key}"
    assert resources["pipelines"]["wind_telemetry_sdp"]["catalog"] == "${var.catalog}"


def test_targets_use_model_name_workspace_folder_and_no_dev_prefix(bundle):
    root, _ = bundle
    for name, target in root["targets"].items():
        assert "${var.model_name}" in target["workspace"]["root_path"], name
        # mode: development would prefix names with "[dev <user>]"
        assert target.get("mode") != "development", name


def test_pipeline_static_parameters_cover_pipeline_config(bundle):
    _, resources = bundle
    conf = resources["pipelines"]["wind_telemetry_sdp"]["configuration"]
    for key in ("wind.catalog", "wind.sites", "wind.landing_root", "wind.silver.dedupe_watermark"):
        assert key in conf
    assert conf["wind.sites"] == "${var.enabled_sites}"


def test_dev_target_enables_subset_of_sites(bundle):
    root, _ = bundle
    from jade_marmot_wind.sites import load_sites, select_sites

    dev_sites = root["targets"]["dev"]["variables"]["enabled_sites"]
    assert 0 < len(select_sites(load_sites(), dev_sites)) < 5


def test_simulator_fans_out_over_planned_sites(bundle):
    _, resources = bundle
    tasks = {t["task_key"]: t for t in resources["jobs"]["wind_test_data_simulator"]["tasks"]}
    fe = tasks["simulate_sites"]["for_each_task"]
    assert fe["inputs"] == "{{tasks.plan_sites.values.sites}}"
    params = fe["task"]["python_wheel_task"]["parameters"]
    assert "--site-id={{input.site_id}}" in params
    assert "--enabled-sites=${var.enabled_sites}" in params


def test_orchestrator_chains_simulator_and_pipeline(bundle):
    _, resources = bundle
    job = resources["jobs"]["wind_telemetry_orchestrator"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert tasks["generate_test_data"]["run_job_task"]["job_id"] == (
        "${resources.jobs.wind_test_data_simulator.id}"
    )
    assert tasks["refresh_pipeline_full"]["pipeline_task"]["full_refresh"] is True
    assert tasks["refresh_pipeline_incremental"]["pipeline_task"]["full_refresh"] is False
    assert tasks["validate_outputs"]["run_if"] == "AT_LEAST_ONE_SUCCESS"

    orchestrator_params = {p["name"] for p in job["parameters"]}
    simulator_params = {
        p["name"] for p in resources["jobs"]["wind_test_data_simulator"]["parameters"]
    }
    passed = set(tasks["generate_test_data"]["run_job_task"]["job_parameters"])
    assert passed <= simulator_params
    assert passed - {"run_id"} <= orchestrator_params
