import os
import json
import signal
import sys
import time

import pika
import psycopg2

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "airflow")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "airflow")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "messages")

PG_HOST = os.getenv("PG_HOST", "postgres-data")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "datauser")
PG_PASS = os.getenv("PG_PASS", "datapass")
PG_DB = os.getenv("PG_DB", "datadb")
PG_TABLE = os.getenv("PG_TABLE", "messages")


class Consumer:
    def __init__(self):
        self.pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
        )
        self.pg_conn.autocommit = True
        self.pg_cursor = self.pg_conn.cursor()

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
        self.channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        self.channel.basic_qos(prefetch_count=10)

        self._stop = False
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self._stop = True

    def _store(self, body: bytes):
        self.pg_cursor.execute(
            f"INSERT INTO {PG_TABLE} (body) VALUES (%s)", (body.decode("utf-8"),)
        )

    def _on_message(self, channel, method, properties, body):
        try:
            self._store(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[x] Stored: {body.decode('utf-8')[:120]}")
        except Exception as exc:
            print(f"[!] Error storing message, rejecting: {exc}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE, on_message_callback=self._on_message
        )
        print(f"[*] Waiting for messages on '{RABBITMQ_QUEUE}'. To exit press CTRL+C")
        while not self._stop:
            try:
                self.connection.process_data_events(time_limit=1)
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
                print("[!] Connection lost, reconnecting...")
                self._reconnect()
                continue

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
                self.channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
                self.channel.basic_qos(prefetch_count=10)
                self.channel.basic_consume(
                    queue=RABBITMQ_QUEUE, on_message_callback=self._on_message
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
    consumer = Consumer()
    consumer.run()
    sys.exit(0)


if __name__ == "__main__":
    main()
