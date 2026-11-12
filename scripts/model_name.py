"""Print {{model_name}}: the git checkout's folder name as a UC identifier.

Used by CI and local deploys:  BUNDLE_VAR_model_name=$(python scripts/model_name.py)
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from jade_marmot_wind.naming import model_name_from_folder  # noqa: E402


def repo_folder() -> str:
    try:
        top = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return Path(top).name
    except (OSError, subprocess.CalledProcessError):
        return ROOT.name


if __name__ == "__main__":
    print(model_name_from_folder(repo_folder()))
