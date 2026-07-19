# AGENTS.md

Airflow + RabbitMQ + PostgreSQL demo. Airflow runs a permanent AMQP consumer
(`pika.basic_consume`) that stores each RabbitMQ message into PostgreSQL in
real time; a separate `producer` service sends test messages.

## Commands

Build and start everything:
```bash
docker compose build
docker compose up -d
```

Send test messages to RabbitMQ:
```bash
docker compose run --rm producer
```

Start the consumer (manual trigger, NOT scheduled):
```bash
docker compose exec airflow-scheduler airflow dags unpause rabbitmq_consumer_daemon
docker compose exec airflow-scheduler airflow dags trigger rabbitmq_consumer_daemon
```

Verify data landed in PostgreSQL:
```bash
docker compose exec postgres-data psql -U datauser -d datadb -c "SELECT id, body FROM messages ORDER BY id DESC LIMIT 5;"
```

Check RabbitMQ queue depth:
```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
```

## Architecture notes (non-obvious)

- The consumer is a standalone script `dags/rabbitmq_consumer.py` (pure `pika` +
  `psycopg2`), launched by the DAG wrapper `dags/rabbitmq_consumer_daemon.py`
  via `BashOperator`. It uses `basic_consume` (push), not scheduled `basic_get`.
- DAG `rabbitmq_consumer_daemon` has `schedule=None` and `max_active_runs=1`: it
  must be triggered manually and runs as a long-lived task until the process ends.
  It auto-exits after `MAX_IDLE_SECONDS` (300) of no messages for a clean restart.
- After editing a DAG file, the scheduler may not register it immediately. If
  `airflow dags trigger <id>` fails with `DagNotFound`, run
  `docker compose exec airflow-scheduler airflow dags reserialize` to sync.
- The old scheduled DAG (`rabbitmq_to_postgres.py`, `basic_get` every minute) was
  removed in favor of the persistent consumer. Do not reintroduce it.

## Env / connections (docker-compose service hostnames)

- RabbitMQ: `rabbitmq:5672`, user `airflow` / pass `airflow`, queue `messages`
- PostgreSQL data: `postgres-data:5432`, db `datadb`, user `datauser` / `datapass`
- Airflow UI: http://localhost:8080 (admin/admin); RabbitMQ UI: http://localhost:15672

## Gotchas

- `_PIP_ADDITIONAL_REQUIREMENTS` installs `pika psycopg2-binary` at container start
  (do NOT use `apache-airflow-providers-rabbitmq` — that package name does not exist;
  the AMQP provider is `apache-airflow-providers-amqp`, and it is not needed here).
- `postgres-data` table `messages` is created by `init-db/01_init.sql` on first start;
  recreating the `data-db-data` volume drops stored messages.
