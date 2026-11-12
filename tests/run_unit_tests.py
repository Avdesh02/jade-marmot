"""Entry point for the `wind_unit_tests` job: runs pytest over the deployed bundle files."""

import argparse
import os
import sys

import pytest

parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True, help="bundle files root, ${workspace.file_path}")
args, _ = parser.parse_known_args()

# Workspace files are read-only from the task's point of view; don't try to write caches.
sys.dont_write_bytecode = True
os.chdir(args.root)
sys.path.insert(0, os.path.join(args.root, "src"))

code = pytest.main(["tests/unit", "-v", "-p", "no:cacheprovider", "--rootdir", args.root])
if code != 0:
    raise SystemExit(f"pytest failed with exit code {code}")
