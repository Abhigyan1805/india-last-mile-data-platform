"""Download raw datasets into DATA_DIR (idempotent; skips files already present).

Kaggle auth: the token is read at runtime from ~/.kaggle/access_token and passed to the kaggle CLI
through the child process environment only. It is never written to the repo, logs or images.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from ingest.common import Settings

# slug -> (local dir, expected csv name, licence as reported by Kaggle metadata)
DATASETS = {
    "sujalsuthar/amazon-delivery-dataset": ("amazon", "amazon_delivery.csv", "MIT"),
    "saurabhbadole/zomato-delivery-operations-analytics-dataset": (
        "zomato", "Zomato Dataset.csv", "other (unspecified)"),
    # Mirror of the India Post "All India Pincode Directory" (data.gov.in) incl. office lat/long.
    "shibin007/all-india-pincode-directory2025": (
        "pincodes", "all_india_pincode_directory_2025.csv", "CC-BY-SA-4.0"),
}


def kaggle_env() -> dict[str, str]:
    env = dict(os.environ)
    token_file = Path.home() / ".kaggle" / "access_token"
    if "KAGGLE_API_TOKEN" not in env and token_file.exists():
        env["KAGGLE_API_TOKEN"] = token_file.read_text().strip()
    return env


def acquire(settings: Settings) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for slug, (sub, csv_name, _licence) in DATASETS.items():
        target = settings.data_dir / "raw" / sub
        csv_path = target / csv_name
        if csv_path.exists():
            print(f"[acquire] {sub}: already present ({csv_path.name})")
        else:
            if shutil.which("kaggle") is None:
                sys.exit("kaggle CLI not found on PATH (pip install kaggle)")
            target.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["kaggle", "datasets", "download", "-d", slug, "--unzip", "-p", str(target), "-q"],
                check=True, env=kaggle_env(),
            )
            print(f"[acquire] {sub}: downloaded {slug}")
        out[sub] = csv_path
    manifest = {slug: {"dir": v[0], "file": v[1], "licence": v[2]} for slug, v in DATASETS.items()}
    (settings.data_dir / "raw").mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "raw" / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))
    return out


if __name__ == "__main__":
    acquire(Settings.from_env())
