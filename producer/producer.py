import os
import json
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "airflow")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "airflow")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "orders")

# Demo payload per queue (consumed by the matching demo DAG).
SAMPLES = {
    "orders": {"order_id": 1, "product": "widget", "quantity": 3},
    "signals": {"signal": "PRICE_UP", "symbol": "AAPL", "value": 195.4},
    "alerts": {"level": "warn", "message": "latency spike on api-1"},
}


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    body = SAMPLES.get(RABBITMQ_QUEUE, {"note": f"message for {RABBITMQ_QUEUE}"})
    channel.basic_publish(
        exchange="",
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(body),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[x] Sent to '{RABBITMQ_QUEUE}': {json.dumps(body)}")
    connection.close()


if __name__ == "__main__":
    main()
