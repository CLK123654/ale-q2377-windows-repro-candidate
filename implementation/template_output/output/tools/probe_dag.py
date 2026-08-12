from __future__ import annotations

import argparse
import json
import re

from airflow import settings
from airflow.exceptions import AirflowClusterPolicySkipDag
from airflow.models import DagBag
from airflow.serialization.serialized_objects import SerializedDAG


def minutes(value):
    return None if value is None else int(value.total_seconds() // 60)


def project(dag):
    return {
        "dag_id": dag.dag_id,
        "catchup": dag.catchup,
        "tags": sorted(dag.tags),
        "tasks": [
            {
                "task_id": task.task_id,
                "task_type": task.task_type,
                "owner": task.owner,
                "queue": task.queue,
                "retries": task.retries,
                "execution_timeout_minutes": minutes(task.execution_timeout),
                "upstream_task_ids": sorted(task.upstream_task_ids),
                "downstream_task_ids": sorted(task.downstream_task_ids),
            }
            for task in sorted(dag.tasks, key=lambda item: item.task_id)
        ],
    }


parser = argparse.ArgumentParser()
parser.add_argument("--dag-file", required=True)
parser.add_argument("--dag-id", required=True)
args = parser.parse_args()
skip_messages = []
original = settings.dag_policy


def record_skip(dag):
    try:
        return original(dag)
    except AirflowClusterPolicySkipDag as error:
        skip_messages.append(str(error))
        raise


settings.dag_policy = record_skip
bag = DagBag(dag_folder=args.dag_file, include_examples=False, safe_mode=False, read_dags_from_db=False)
if bag.import_errors:
    message = next(iter(bag.import_errors.values()))
    code = re.search(r"(TAG_CONTRACT|OWNER_MISMATCH|FORBIDDEN_BASH|CATCHUP_ENABLED|TASK_LIMIT)", message)
    payload = {"decision": "REJECT", "reason": code.group(1) if code else "IMPORT_ERROR", "loaded": False, "import_error": message.splitlines()[-1]}
elif args.dag_id not in bag.dags:
    payload = {"decision": "QUARANTINE", "reason": "QUARANTINED" if skip_messages else "NOT_LOADED", "loaded": False, "import_error": ""}
else:
    dag = bag.dags[args.dag_id]
    source = project(dag)
    restored = project(SerializedDAG.from_dict(SerializedDAG.to_dict(dag)))
    payload = {"decision": "ADMIT", "reason": "", "loaded": True, "import_error": "", "source_projection": source, "roundtrip_projection": restored, "roundtrip_equal": source == restored}
print("ALE_RESULT=" + json.dumps(payload, ensure_ascii=False, sort_keys=True))
