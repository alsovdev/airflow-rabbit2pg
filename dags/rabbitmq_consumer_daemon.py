# DEPRECATED: long-running anti-pattern (Airflow worker slot blocked indefinitely).
# Consumer liveness is now monitored and auto-restarted by the rabbitmq_watchdog DAG.
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="rabbitmq_consumer_daemon",
    default_args=default_args,
    description="DEPRECATED long-running RabbitMQ consumer (legacy, auto-restarted by rabbitmq_watchdog)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rabbitmq", "postgres", "consumer", "daemon"],
    max_active_runs=1,
) as dag:
    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="python /opt/airflow/dags/rabbitmq_consumer.py",
        execution_timeout=None,
        env={
            "RABBITMQ_HOST": "rabbitmq",
            "RABBITMQ_PORT": "5672",
            "RABBITMQ_USER": "airflow",
            "RABBITMQ_PASS": "airflow",
            "RABBITMQ_QUEUE": "messages",
            "PG_HOST": "postgres-data",
            "PG_PORT": "5432",
            "PG_USER": "datauser",
            "PG_PASS": "datapass",
            "PG_DB": "datadb",
            "PG_TABLE": "messages",
            "MAX_IDLE_SECONDS": "300",
        },
    )
