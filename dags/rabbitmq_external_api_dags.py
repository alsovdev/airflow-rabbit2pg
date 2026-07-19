"""
Example DAG template: triggered by the rabbitmq-consumer service via the Airflow
REST API. Each run receives the RabbitMQ message body as dag_run.conf and calls
an external REST service (with that service's OWN authorization - stubbed here).

This file generates the 3 demo DAGs (demo_orders, demo_signals, demo_alerts) and
documents how to scale to ~100 DAGs using the same factory.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# External service endpoint + auth are placeholders. In production these come
# from Airflow Connections / Variables, never hardcoded.
EXTERNAL_API_URL = "https://external-service.example.com/api/v1/events"
EXTERNAL_API_TOKEN = "REPLACE_WITH_SECRET_FROM_AIRFLOW_CONNECTION"


def call_external_api(**context):
    """Stub: POST the RabbitMQ message (dag_run.conf) to the external REST API."""
    # The message body forwarded by the consumer service lands in dag_run.conf.
    payload = context["dag_run"].conf or {}

    # --- stubbed external call (replace with real auth/endpoint) ---
    import requests

    print(f"[demo] POST {EXTERNAL_API_URL} with payload: {payload}")
    # resp = requests.post(
    #     EXTERNAL_API_URL,
    #     json=payload,
    #     headers={"Authorization": f"Bearer {EXTERNAL_API_TOKEN}"},
    #     timeout=30,
    # )
    # resp.raise_for_status()
    # print(f"[demo] External API responded {resp.status_code}")
    print("[demo] (stub) external API call skipped - implement auth + endpoint")


def make_dag(dag_id: str, description: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(seconds=30)},
        schedule=None,  # only triggered via API by the consumer service
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["external-api", "demo", "event-driven"],
    ) as dag:
        PythonOperator(
            task_id="call_external_api",
            python_callable=call_external_api,
        )
    return dag


# --- Demo DAGs (3 queues) ---
make_dag("demo_orders", "Demo: orders queue -> external orders API")
make_dag("demo_signals", "Demo: signals queue -> external signals API")
make_dag("demo_alerts", "Demo: alerts queue -> external alerts API")

# --- Scale to ~100 DAGs ---
# Generate one DAG per queue_N/dag_N pair from the same mapping used by the
# consumer service. Keep this in sync with consumer/queue_dag_map.json.
import json
from pathlib import Path

_MAP = Path(__file__).parent.parent / "consumer" / "queue_dag_map.json"
if _MAP.exists():
    data = json.loads(_MAP.read_text())
    for queue, dag_id in data.items():
        if queue.startswith("_"):
            continue
        if dag_id.startswith("demo_"):
            continue  # already created above
        make_dag(dag_id, f"Generated: {queue} queue -> external API")
