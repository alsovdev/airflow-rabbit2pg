"""
Watchdog: monitors the standalone rabbitmq-consumer service (which runs OUTSIDE
Airflow) and alerts if it is dead. The service self-heals via docker-compose
`restart: unless-stopped` / k8s ReplicaSet, so this DAG only reports, it does NOT
restart the consumer (the consumer is no longer an Airflow DAG).
"""
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

PG_HOST = os.getenv("PG_HOST", "postgres-data")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "datauser")
PG_PASS = os.getenv("PG_PASS", "datapass")
PG_DB = os.getenv("PG_DB", "datadb")
HEARTBEAT_TABLE = os.getenv("HEARTBEAT_TABLE", "consumer_heartbeat")

WATCHDOG_THRESHOLD_SECONDS = int(os.getenv("WATCHDOG_THRESHOLD_SECONDS", "60"))


def _last_heartbeat():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(last_seen) FROM {HEARTBEAT_TABLE}")
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def check_consumer(**context):
    last_seen = _last_heartbeat()

    if last_seen is None:
        raise AirflowException("[watchdog] No heartbeat yet - consumer service never started!")

    age = (datetime.now(last_seen.tzinfo) - last_seen).total_seconds()
    print(f"[watchdog] Last consumer heartbeat: {last_seen} (age {age:.0f}s)")

    if age <= WATCHDOG_THRESHOLD_SECONDS:
        print("[watchdog] Consumer service is alive.")
        return

    raise AirflowException(
        f"[watchdog] Consumer service DEAD for {age:.0f}s (> {WATCHDOG_THRESHOLD_SECONDS}s). "
        "Alert! The service should self-heal via restart policy; investigate if it stays down."
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
