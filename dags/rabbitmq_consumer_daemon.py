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
    description="Long-running RabbitMQ consumer that stores messages into PostgreSQL",
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
