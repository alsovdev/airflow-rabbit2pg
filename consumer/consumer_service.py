"""
RabbitMQ -> Airflow event router (runs OUTSIDE Airflow as a standalone service).

For each message consumed from a queue, this service triggers the mapped Airflow
DAG via the Airflow REST API (POST /api/v1/dags/{dag_id}/dagRuns) and passes the
message body as the DAG run conf. The DAG then calls the external REST service.

Why outside Airflow:
  A long-running consumer must NOT live inside an Airflow worker (blocks a slot,
  dies on worker restart). This service is managed by docker-compose
  (restart: unless-stopped) / k8s ReplicaSet, so it self-heals independently.

Liveness:
  The service exposes GET /healthz returning the age of the last processed event.
  The rabbitmq_watchdog DAG polls this endpoint instead of a database heartbeat,
  so the consumer holds NO connection to PostgreSQL at all.

Static queue->DAG mapping is provided via QUEUE_DAG_MAP (JSON string or file path).
"""
import os
import sys
import time
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pika
import requests

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

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8081"))
# If no event processed for this many seconds, report unhealthy.
HEALTH_THRESHOLD_SECONDS = int(os.getenv("HEALTH_THRESHOLD_SECONDS", "60"))

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


class HealthState:
    """Shared, thread-safe liveness state updated by the consumer loop."""

    def __init__(self):
        self._lock = threading.Lock()
        self._last_seen = time.time()

    def touch(self):
        with self._lock:
            self._last_seen = time.time()

    def age(self):
        with self._lock:
            return time.time() - self._last_seen


class HealthHandler(BaseHTTPRequestHandler):
    health = None  # set in main()

    def do_GET(self):
        if self.path != "/healthz":
            self.send_response(404)
            self.end_headers()
            return
        age = HealthHandler.health.age()
        if age <= HEALTH_THRESHOLD_SECONDS:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "age": age}).encode())
        else:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "dead", "age": age}).encode())

    def log_message(self, *args):  # silence default logging
        pass


def start_health_server(health: HealthState):
    HealthHandler.health = health
    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


class Router:
    def __init__(self, queue_dag_map: dict, health: HealthState):
        self.queue_dag_map = queue_dag_map
        self.health = health
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

        self.session = requests.Session()
        self.session.auth = (AIRFLOW_USER, AIRFLOW_PASS)

    def _handle_signal(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self._stop = True

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

        # Mark liveness on every received message (proves the consumer is alive).
        self.health.touch()

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
            # Liveness = the process is alive and looping, independent of traffic.
            self.health.touch()

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


def main():
    queue_dag_map = load_queue_dag_map()
    print(f"[router] Loaded {len(queue_dag_map)} queue->DAG mappings")
    health = HealthState()
    start_health_server(health)
    Router(queue_dag_map, health).run()
    sys.exit(0)


if __name__ == "__main__":
    main()
