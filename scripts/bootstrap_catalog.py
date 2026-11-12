"""Create the target's Unity Catalog catalog if it doesn't exist.

The bundle can't own the catalog on this workspace (see resources/unity_catalog.yml), so
this runs before `databricks bundle deploy`. It goes through SQL `CREATE CATALOG IF NOT
EXISTS` on a SQL warehouse, which works with Default Storage where the REST API doesn't.

    python scripts/bootstrap_catalog.py --target dev [--profile wind] [--warehouse-id ID]

Uses the Databricks CLI for auth, so it works with a profile locally and with
DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET in CI.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from model_name import model_name_from_folder, repo_folder  # noqa: E402

from jade_marmot_wind.naming import catalog_name, check_identifier  # noqa: E402


def cli(*args: str, profile: str | None) -> dict | list:
    cmd = ["databricks", *args, "-o", "json"]
    if profile:
        cmd += ["-p", profile]
    # Stop Git Bash on Windows from rewriting /api/... into a filesystem path.
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    out = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if out.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed:\n{out.stderr}")
    return json.loads(out.stdout or "{}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--target", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--warehouse-id", default=None, help="default: first warehouse listed")
    p.add_argument(
        "--model-name",
        default=os.environ.get("BUNDLE_VAR_model_name") or model_name_from_folder(repo_folder()),
    )
    args = p.parse_args()

    catalog = check_identifier(catalog_name(args.model_name, args.target), "catalog")
    existing = {c["name"] for c in cli("catalogs", "list", profile=args.profile)}
    if catalog in existing:
        print(f"catalog {catalog} already exists")
        return

    warehouse_id = args.warehouse_id
    if not warehouse_id:
        warehouses = cli("warehouses", "list", profile=args.profile)
        if not warehouses:
            raise SystemExit("no SQL warehouse available; pass --warehouse-id")
        warehouse_id = warehouses[0]["id"]

    body = {
        "warehouse_id": warehouse_id,
        "statement": f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT "
        f"'Wind turbine telemetry ({args.model_name}, {args.target}). "
        "Bootstrapped for the jade_marmot bundle.'",
        "wait_timeout": "50s",
    }
    result = cli(
        "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body), profile=args.profile
    )
    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise SystemExit(f"CREATE CATALOG {catalog} ended in state {state}: {result}")
    print(f"created catalog {catalog}")


if __name__ == "__main__":
    main()
