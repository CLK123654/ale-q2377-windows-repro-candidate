from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("--input-dir", required=True)
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()
input_dir = Path(args.input_dir).resolve()
output_dir = Path(args.output_dir).resolve()
if output_dir.exists():
    shutil.rmtree(output_dir)
try:
    contract = json.loads((input_dir / "contracts/cluster_policy.json").read_text(encoding="utf-8"))
    inventory = json.loads((input_dir / "registry/dag_inventory.json").read_text(encoding="utf-8"))
    names = [row["source_file"] for row in inventory]
    if len(names) != len(set(names)):
        raise ValueError("source_file存在重复")
    if set(names) != {path.name for path in (input_dir / "dags").glob("*.py")}:
        raise ValueError("接入清单与DAG源码包不一致")
    output_dir.mkdir(parents=True)
    probe = Path(__file__).with_name("probe_dag.py")
    policy_dir = Path(__file__).resolve().parents[1] / "policy"
    env = os.environ.copy()
    env.update({
        "AIRFLOW_HOME": tempfile.mkdtemp(prefix="airflow_policy_"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "ALE_POLICY_CONTRACT": str(input_dir / "contracts/cluster_policy.json"),
        "PYTHONPATH": os.pathsep.join([str(policy_dir), env.get("PYTHONPATH", "")]).rstrip(os.pathsep),
    })
    decisions = []
    effective = []
    graphs = {}
    for item in inventory:
        dag_file = input_dir / "dags" / item["source_file"]
        command = [sys.executable, str(probe), "--dag-file", str(dag_file), "--dag-id", dag_file.stem]
        run = subprocess.run(command, text=True, capture_output=True, env=env, timeout=180, check=False)
        marker = next((line for line in reversed(run.stdout.splitlines()) if line.startswith("ALE_RESULT=")), None)
        if run.returncode != 0 or marker is None:
            raise RuntimeError(run.stdout + run.stderr)
        observed = json.loads(marker.split("=", 1)[1])
        decisions.append({"source_file": item["source_file"], "dag_id": dag_file.stem, "service": item["service"], "business_owner": item["business_owner"], "decision": observed["decision"], "reason": observed["reason"], "dagbag_loaded": str(observed["loaded"]).lower(), "import_error": observed["import_error"]})
        if observed["decision"] != "ADMIT":
            continue
        projection = observed["source_projection"]
        if not observed["roundtrip_equal"]:
            raise ValueError("SerializedDAG往返结果不一致")
        graphs[projection["dag_id"]] = {"source": projection, "roundtrip": observed["roundtrip_projection"], "roundtrip_equal": True}
        for task in projection["tasks"]:
            effective.append({"dag_id": projection["dag_id"], "task_id": task["task_id"], "owner": task["owner"], "queue": task["queue"], "retries": task["retries"], "execution_timeout_minutes": task["execution_timeout_minutes"]})
    headers = ["source_file", "dag_id", "service", "business_owner", "decision", "reason", "dagbag_loaded", "import_error"]
    with (output_dir / "dag_admission.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, lineterminator="\n"); writer.writeheader(); writer.writerows(decisions)
    headers = ["dag_id", "task_id", "owner", "queue", "retries", "execution_timeout_minutes"]
    with (output_dir / "task_effective_policy.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, lineterminator="\n"); writer.writeheader(); writer.writerows(effective)
    (output_dir / "serialized_dags.json").write_text(json.dumps(graphs, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = {
        "contract_version": contract["contract_version"],
        "result": "READY" if all(row["decision"] in {"ADMIT", "QUARANTINE", "REJECT"} for row in decisions) else "INCOMPLETE",
        "decision_counts": {value: sum(row["decision"] == value for row in decisions) for value in ["ADMIT", "QUARANTINE", "REJECT"]},
        "reason_counts": {value: sum(row["reason"] == value for row in decisions) for value in sorted({row["reason"] for row in decisions if row["reason"]})},
        "admitted_dags": sorted(row["dag_id"] for row in decisions if row["decision"] == "ADMIT"),
        "effective_task_keys": sorted(f"{row['dag_id']}::{row['task_id']}" for row in effective),
        "serialization_roundtrips_equal": all(item["roundtrip_equal"] for item in graphs.values()),
    }
    (output_dir / "handoff_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
except Exception:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    raise
