import json
import os
from datetime import timedelta

from airflow.exceptions import AirflowClusterPolicySkipDag, AirflowClusterPolicyViolation


with open(os.environ["ALE_POLICY_CONTRACT"], encoding="utf-8") as handle:
    CONTRACT = json.load(handle)


def _team(dag):
    prefix = CONTRACT["team_tag_prefix"]
    values = [tag[len(prefix):] for tag in dag.tags if tag.startswith(prefix)]
    return values[0] if len(values) == 1 and values[0] in CONTRACT["teams"] else None


def _forbidden(task):
    if task.task_type != "BashOperator":
        return None
    command = str(getattr(task, "bash_command", "")).lower()
    return next((token for token in CONTRACT["forbidden_bash_tokens"] if token in command), None)


def _task_violation(task, team):
    if task.owner != team:
        return "OWNER_MISMATCH"
    if _forbidden(task) is not None:
        return "FORBIDDEN_BASH"
    return None


def task_policy(task):
    team = _team(task.dag)
    if team is None:
        return
    violation = _task_violation(task, team)
    if violation:
        raise AirflowClusterPolicyViolation(violation)
    rules = CONTRACT["teams"][team]
    task.queue = rules["queue"]
    task.retries = min(task.retries, rules["max_retries"])
    limit = timedelta(minutes=rules["max_execution_minutes"])
    if task.execution_timeout is None or task.execution_timeout > limit:
        task.execution_timeout = limit


def dag_policy(dag):
    if CONTRACT["quarantine_tag"] in dag.tags:
        raise AirflowClusterPolicySkipDag("QUARANTINED")
    team = _team(dag)
    if team is None or CONTRACT["environment_tag"] not in dag.tags:
        raise AirflowClusterPolicyViolation("TAG_CONTRACT")
    for task in dag.tasks:
        violation = _task_violation(task, team)
        if violation:
            raise AirflowClusterPolicyViolation(violation)
    if dag.catchup:
        raise AirflowClusterPolicyViolation("CATCHUP_ENABLED")
    if len(dag.tasks) > CONTRACT["limits"]["max_tasks_per_dag"]:
        raise AirflowClusterPolicyViolation("TASK_LIMIT")
