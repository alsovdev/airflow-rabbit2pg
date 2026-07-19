import os
import subprocess
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

LEGACY_DAG_ID = os.getenv("LEGACY_DAG_ID", "rabbitmq_consumer_daemon")
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


def _trigger_legacy_consumer():
    print(f"[watchdog] Triggering legacy DAG '{LEGACY_DAG_ID}'...")
    result = subprocess.run(
        ["airflow", "dags", "trigger", LEGACY_DAG_ID],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout)


def check_and_heal(**context):
    last_seen = _last_heartbeat()

    if last_seen is None:
        print("[watchdog] No heartbeat yet -> consumer never started, triggering.")
        _trigger_legacy_consumer()
        return

    age = (datetime.now(last_seen.tzinfo) - last_seen).total_seconds()
    print(f"[watchdog] Last consumer heartbeat: {last_seen} (age {age:.0f}s)")

    if age <= WATCHDOG_THRESHOLD_SECONDS:
        print("[watchdog] Consumer is alive, nothing to do.")
        return

    print(
        f"[watchdog] Consumer DEAD for {age:.0f}s (> {WATCHDOG_THRESHOLD_SECONDS}s), restarting."
    )
    try:
        _trigger_legacy_consumer()
    except subprocess.CalledProcessError as exc:
        raise AirflowException(f"[watchdog] Failed to restart consumer: {exc.stderr}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="rabbitmq_watchdog",
    default_args=default_args,
    description="Watchdog: checks consumer heartbeat and auto-restarts the legacy RabbitMQ consumer DAG",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rabbitmq", "watchdog", "monitoring"],
    max_active_runs=1,
) as dag:
    check_consumer = PythonOperator(
        task_id="check_and_heal",
        python_callable=check_and_heal,
        execution_timeout=timedelta(minutes=2),
    )
