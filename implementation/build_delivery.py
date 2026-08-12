from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "template_output" / "output"
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
output = Path(args.output).resolve()
if output.exists():
    shutil.rmtree(output)
try:
    shutil.copytree(TEMPLATE, output)
    result_dir = output / "results"
    run = subprocess.run([sys.executable, str(output / "tools" / "audit_policy.py"), "--input-dir", str(Path(args.input).resolve()), "--output-dir", str(result_dir)], text=True, capture_output=True, timeout=600, check=False)
    if run.returncode != 0:
        raise RuntimeError(run.stdout + run.stderr)
except Exception:
    if output.exists():
        shutil.rmtree(output)
    raise
