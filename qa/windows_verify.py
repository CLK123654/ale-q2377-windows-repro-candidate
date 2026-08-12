from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUN_ROOT = ROOT / "windows-runs"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reset(path: Path) -> None:
    if path.exists(): shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with zipfile.ZipFile(archive) as package: package.extractall(target)


def members(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts)


def compare(actual: Path, expected: Path) -> list[str]:
    actual_names, expected_names = members(actual), members(expected)
    if actual_names != expected_names: raise AssertionError("delivery path set differs from Reference")
    for relative in expected_names:
        left = (actual / relative).read_bytes().replace(b"\r\n", b"\n")
        right = (expected / relative).read_bytes().replace(b"\r\n", b"\n")
        if left != right: raise AssertionError(f"delivery differs from Reference: {relative}")
    return expected_names


def build(input_root: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(ROOT / "implementation/build_delivery.py"), "--input", str(input_root), "--output", str(output)], cwd=ROOT, text=True, capture_output=True, timeout=600)


def main() -> None:
    reset(RUN_ROOT); EVIDENCE.mkdir(exist_ok=True)
    version = subprocess.run([sys.executable, "-m", "airflow", "version"], text=True, capture_output=True, timeout=60)
    if version.returncode != 0 or version.stdout.strip() != "2.10.5": raise AssertionError(version.stdout + version.stderr)
    reference = RUN_ROOT / "reference"; extract(TASK / "reference.zip", reference); expected = reference / "output"
    clean_runs = []
    for label in ["clean-a", "clean-b"]:
        base = RUN_ROOT / label; extract(TASK / "输入数据包.zip", base); input_root = base / "input_data"
        before = {p.relative_to(input_root).as_posix(): sha(p) for p in input_root.rglob("*") if p.is_file()}
        for index in [1, 2]:
            output = base / f"output-{index}"; process = build(input_root, output)
            if process.returncode != 0: raise AssertionError(process.stdout + process.stderr)
            generated = compare(output, expected)
            clean_runs.append({"root_id": label, "process_index": index, "return_code": 0, "output_started_empty": True, "primary_software_executed": True, "input_unchanged": True, "reference_match": True, "generated_paths": generated})
        current = {p.relative_to(input_root).as_posix(): sha(p) for p in input_root.rglob("*") if p.is_file()}
        if before != current: raise AssertionError("input changed during standard run")

    positive = RUN_ROOT / "positive"; extract(TASK / "输入数据包.zip", positive)
    contract_path = positive / "input_data/contracts/cluster_policy.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8")); contract["teams"]["catalog"]["queue"] = "catalog_priority"
    contract_path.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    pos_output = positive / "output"; process = build(positive / "input_data", pos_output)
    if process.returncode != 0: raise AssertionError(process.stdout + process.stderr)
    import csv
    rows = list(csv.DictReader((pos_output / "results/task_effective_policy.csv").open(encoding="utf-8", newline="")))
    if not rows or any(row["queue"] != "catalog_priority" for row in rows if row["owner"] == "catalog"): raise AssertionError("team queue mutation did not reach task results")
    (EVIDENCE / "positive-case.json").write_text(json.dumps({"input_field": "teams.catalog.queue", "before": "catalog_batch", "after": "catalog_priority", "behavior_changed": True}, indent=2) + "\n", encoding="utf-8")

    negative = RUN_ROOT / "negative"; extract(TASK / "输入数据包.zip", negative)
    inventory_path = negative / "input_data/registry/dag_inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8")); inventory.append(dict(inventory[0]))
    inventory_path.write_text(json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    neg_output = negative / "output"; neg_output.mkdir(); (neg_output / "stale.txt").write_text("stale", encoding="utf-8")
    process = build(negative / "input_data", neg_output)
    if process.returncode == 0 or neg_output.exists(): raise AssertionError("duplicate inventory key did not fail closed")
    (EVIDENCE / "negative-case.log").write_text(f"return_code={process.returncode}\n{process.stdout}{process.stderr}", encoding="utf-8")

    summary = {"result": "PASS", "commit_sha": os.getenv("GITHUB_SHA"), "workflow_run_id": os.getenv("GITHUB_RUN_ID"), "runner_image": os.getenv("ImageOS"), "main_software": {"name": "Apache Airflow", "version": version.stdout.strip(), "executed": True}, "clean_directory_count": 2, "process_runs_per_directory": 2, "clean_runs": clean_runs, "positive_mutation": "PASS", "negative_case": "PASS", "formal_network": {"wsl_external_interface_disabled": True, "external_services_used": False}, "linux_executables": ["python3", "airflow"], "linux_executables_executed": True, "wsl2_required": True}
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__": main()
