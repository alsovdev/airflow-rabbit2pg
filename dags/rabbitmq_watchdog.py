"""
Watchdog: monitors the standalone rabbitmq-consumer service (which runs OUTSIDE
Airflow) and alerts if it is dead. The service self-heals via docker-compose
`restart: unless-stopped` / k8s ReplicaSet, so this DAG only reports, it does NOT
restart the consumer.

Liveness is checked by polling the consumer's HTTP health endpoint (GET /healthz)
instead of a database heartbeat table.
"""
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

CONSUMER_HEALTH_URL = os.getenv(
    "CONSUMER_HEALTH_URL", "http://rabbitmq-consumer:8081/healthz"
)
WATCHDOG_THRESHOLD_SECONDS = int(os.getenv("WATCHDOG_THRESHOLD_SECONDS", "60"))


def check_consumer(**context):
    try:
        resp = requests.get(CONSUMER_HEALTH_URL, timeout=5)
    except requests.RequestException as exc:
        raise AirflowException(f"[watchdog] Cannot reach consumer at {CONSUMER_HEALTH_URL}: {exc}")

    if resp.status_code != 200:
        raise AirflowException(
            f"[watchdog] Consumer health endpoint returned HTTP {resp.status_code}: {resp.text[:200]}"
        )

    data = resp.json()
    age = data.get("age", 0)
    print(f"[watchdog] Consumer alive, last event age: {age:.0f}s")
    if age > WATCHDOG_THRESHOLD_SECONDS:
        raise AirflowException(
            f"[watchdog] Consumer alive but STALE: last event {age:.0f}s ago "
            f"(> {WATCHDOG_THRESHOLD_SECONDS}s). Investigate."
        )


with DAG(
    dag_id="rabbitmq_watchdog",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)},
    description="Watchdog: alerts if the standalone RabbitMQ consumer service is dead",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rabbitmq", "watchdog", "monitoring"],
    max_active_runs=1,
) as dag:
    check_consumer_task = PythonOperator(
        task_id="check_consumer",
        python_callable=check_consumer,
        execution_timeout=timedelta(minutes=2),
    )
