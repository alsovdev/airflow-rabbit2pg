"""
RabbitMQ -> Airflow event router (runs OUTSIDE Airflow as a standalone service).

For each message consumed from a queue, this service triggers the mapped Airflow
DAG via the Airflow REST API (POST /api/v1/dags/{dag_id}/dagRuns) and passes the
message body as the DAG run conf. The DAG then calls the external REST service.

Why outside Airflow:
  A long-running consumer must NOT live inside an Airflow worker (blocks a slot,
  dies on worker restart). This service is managed by docker-compose
  (restart: unless-stopped) / k8s ReplicaSet, so it self-heals independently.

Static queue->DAG mapping is provided via QUEUE_DAG_MAP (JSON string or file path).
"""
import os
import sys
import time
import json
import signal
from pathlib import Path

import pika
import requests
import psycopg2

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "airflow")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "airflow")

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")

# Static mapping: queue name -> Airflow dag_id.
# Accepts either a JSON string or a path to a JSON file.
QUEUE_DAG_MAP_RAW = os.getenv(
    "QUEUE_DAG_MAP",
    '{"orders": "demo_orders", "signals": "demo_signals", "alerts": "demo_alerts"}',
)

HEARTBEAT_TABLE = os.getenv("HEARTBEAT_TABLE", "consumer_heartbeat")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))

PG_HOST = os.getenv("PG_HOST", "postgres-data")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "datauser")
PG_PASS = os.getenv("PG_PASS", "datapass")
PG_DB = os.getenv("PG_DB", "datadb")

# Requeue the message if the Airflow API call fails, so no event is lost.
API_RETRY_LIMIT = int(os.getenv("API_RETRY_LIMIT", "5"))


def load_queue_dag_map():
    raw = QUEUE_DAG_MAP_RAW.strip()
    if raw.startswith("{") or raw.startswith("["):
        return json.loads(raw)
    path = Path(raw)
    if path.exists():
        return json.loads(path.read_text())
    raise RuntimeError(f"Cannot parse QUEUE_DAG_MAP: not JSON and not a file: {raw!r}")


class Router:
    def __init__(self, queue_dag_map: dict):
        self.queue_dag_map = queue_dag_map
        self._stop = False
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=60,
            )
        )
        self.channel = self.connection.channel()
        for queue in self.queue_dag_map:
            self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_qos(prefetch_count=10)

        self.pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
        )
        self.pg_conn.autocommit = True
        self.pg_cursor = self.pg_conn.cursor()
        self._last_heartbeat = 0.0
        self._write_heartbeat()

        self.session = requests.Session()
        self.session.auth = (AIRFLOW_USER, AIRFLOW_PASS)

    def _handle_signal(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self._stop = True

    def _write_heartbeat(self):
        self.pg_cursor.execute(
            f"INSERT INTO {HEARTBEAT_TABLE} (last_seen) VALUES (NOW())"
        )
        self._last_heartbeat = time.time()

    def _trigger_dag(self, dag_id: str, payload: dict) -> bool:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
        for attempt in range(1, API_RETRY_LIMIT + 1):
            try:
                resp = self.session.post(url, json={"conf": payload}, timeout=10)
                if resp.status_code in (200, 201):
                    print(f"[router] Triggered DAG {dag_id} (run_id from response)")
                    return True
                print(f"[router] DAG trigger {dag_id} HTTP {resp.status_code}: {resp.text[:200]}")
            except requests.RequestException as exc:
                print(f"[router] DAG trigger {dag_id} error (attempt {attempt}): {exc}")
            time.sleep(min(2 * attempt, 10))
        return False

    def _on_message(self, channel, method, properties, body):
        queue = method.routing_key
        dag_id = self.queue_dag_map.get(queue)
        if not dag_id:
            print(f"[router] No DAG mapped for queue {queue!r}, dropping (nack no requeue)")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {"raw": body.decode("utf-8", errors="replace")}

        if self._trigger_dag(dag_id, payload):
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # API failed after retries -> requeue so the event is not lost.
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        for queue in self.queue_dag_map:
            self.channel.basic_consume(
                queue=queue, on_message_callback=self._on_message
            )
            print(f"[router] Consuming from queue '{queue}' -> DAG '{self.queue_dag_map[queue]}'")
        print("[router] Waiting for messages. Ctrl+C to exit.")
        while not self._stop:
            try:
                self.connection.process_data_events(time_limit=1)
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
                print("[router] RabbitMQ connection lost, reconnecting...")
                self._reconnect()
                continue
            if time.time() - self._last_heartbeat >= HEARTBEAT_INTERVAL:
                self._write_heartbeat()
        self.close()

    def _reconnect(self):
        for _ in range(10):
            try:
                time.sleep(2)
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=credentials,
                        heartbeat=60,
                    )
                )
                self.channel = self.connection.channel()
                for queue in self.queue_dag_map:
                    self.channel.queue_declare(queue=queue, durable=True)
                self.channel.basic_qos(prefetch_count=10)
                for queue in self.queue_dag_map:
                    self.channel.basic_consume(
                        queue=queue, on_message_callback=self._on_message
                    )
                return
            except Exception:
                continue
        raise RuntimeError("Unable to reconnect to RabbitMQ")

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception:
            pass
        try:
            self.pg_cursor.close()
            self.pg_conn.close()
        except Exception:
            pass


def main():
    queue_dag_map = load_queue_dag_map()
    print(f"[router] Loaded {len(queue_dag_map)} queue->DAG mappings")
    Router(queue_dag_map).run()
    sys.exit(0)


if __name__ == "__main__":
    main()
