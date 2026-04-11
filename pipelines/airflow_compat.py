"""Airflow 2.x / 3.x compatibility for DAG definitions."""

from __future__ import annotations

import airflow

_major = int(airflow.__version__.split(".", 1)[0])
IS_AIRFLOW_3 = _major >= 3


def dag_schedule_kwargs(cron: str) -> dict:
    if IS_AIRFLOW_3:
        return {"schedule": cron}
    return {"schedule_interval": cron}


if IS_AIRFLOW_3:
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,
        PythonOperator,
        ShortCircuitOperator,
    )
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.task.trigger_rule import TriggerRule
else:
    from airflow.operators.python import (
        BranchPythonOperator,
        PythonOperator,
        ShortCircuitOperator,
    )
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.trigger_rule import TriggerRule
