import os
import json
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "airflow")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "airflow")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "messages")


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    for i in range(5):
        message = json.dumps(
            {
                "order_id": i + 1,
                "product": f"product-{i + 1}",
                "quantity": (i + 1) * 10,
            }
        )
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"[x] Sent: {message}")

    connection.close()


if __name__ == "__main__":
    main()
