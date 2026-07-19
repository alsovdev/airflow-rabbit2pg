# Apache Airflow + RabbitMQ + PostgreSQL Demo

Пайплайн, в котором Apache Airflow запускает постоянный long-running consumer
на базе AMQP (`pika.basic_consume`): процесс непрерывно слушает очередь RabbitMQ
и в реальном времени сохраняет каждое сообщение в таблицу PostgreSQL. Отдельный
producer отправляет тестовые данные в RabbitMQ. Шедулер НЕ опрашивает очередь —
consumer живёт как долгоживущая задача и реагирует на сообщения сразу.

## Сервисы (docker-compose.yml)

| Сервис             | Назначение                                              | Порты        |
|--------------------|---------------------------------------------------------|--------------|
| `postgres-airflow` | Метаданные Airflow (бэкенд БД + Celery result backend)  | 5432 (внутр) |
| `postgres-data`    | Хранилище сообщений (`datadb`, таблица `messages`)      | 5432 (внутр) |
| `rabbitmq`         | Брокер сообщений (очередь `messages`) + management UI   | 5672, 15672  |
| `redis`            | Брокер Celery для Airflow executor                      | 6379 (внутр) |
| `airflow-webserver`| Веб-интерфейс Airflow                                   | 8080         |
| `airflow-scheduler`| Планировщик DAG                                         | —            |
| `airflow-worker`   | Celery-воркер, исполняющий таски                         | —            |
| `airflow-init`     | Миграция БД и создание пользователя admin               | —            |
| `producer`         | Пример-приложение, отправляющее сообщения в RabbitMQ    | —            |

## Структура

```
docker-compose.yml
dags/rabbitmq_consumer.py         # standalone AMQP consumer (pika.basic_consume -> Postgres)
dags/rabbitmq_consumer_daemon.py  # DAG-обёртка: запускает consumer как long-running задачу
producer/producer.py              # Пример producer-а
producer/Dockerfile
airflow/Dockerfile                # Кастомный образ Airflow
init-db/01_init.sql               # Создание таблицы messages
```

## Поток данных

```
producer.py ──publish──> RabbitMQ (queue: messages)
                               │
            Airflow DAG run (long-running task, basic_consume)
            процесс слушает очередь постоянно и пишет каждое сообщение сразу
                               │
                               ▼
                  PostgreSQL datadb.messages (body TEXT)
```

## Запуск

```bash
docker compose build
docker compose up -d
# consumer запускается как долгоживущая задача (schedule=None):
docker compose exec airflow-scheduler airflow dags unpause rabbitmq_consumer_daemon
docker compose exec airflow-scheduler airflow dags trigger rabbitmq_consumer_daemon
# в отдельном терминале отправляем сообщения — они попадут в БД мгновенно:
docker compose run --rm producer
```

DAG `rabbitmq_consumer_daemon` имеет `schedule=None` и `max_active_runs=1`:
он не планируется шедулером, а запускается вручную и работает, пока жив процесс.
Если процесс упал/завершился по idle-таймауту (300с без сообщений), перезапустите
run вручную. Graceful shutdown по SIGTERM (кнопка Clear/конец run).

## Проверка

Очередь в RabbitMQ:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
```

Таблица в PostgreSQL:

```bash
docker compose exec postgres-data psql -U datauser -d datadb -c "SELECT id, body, received_at FROM messages ORDER BY id;"
```

## Доступы

- Airflow UI: http://localhost:8080 — `admin` / `admin`
- RabbitMQ UI: http://localhost:15672 — `airflow` / `airflow`

## Подключения (хосты внутри сети docker-compose)

- RabbitMQ: `rabbitmq:5672`, user `airflow` / pass `airflow`, queue `messages`
- PostgreSQL data: `postgres-data:5432`, db `datadb`, user `datauser` / `datapass`

## Заметки

- Consumer — это standalone-скрипт `dags/rabbitmq_consumer.py` (чистые `pika`
  + `psycopg2`), запускаемый DAG-обёрткой через `BashOperator`. Он использует
  `basic_consume` (push-модель), а не `basic_get` по расписанию.
- Доп. зависимости ставятся через `_PIP_ADDITIONAL_REQUIREMENTS` (pika, psycopg2-binary).
- Таблица `messages` создаётся автоматически init-скриптом `init-db/01_init.sql`
  при первом старте `postgres-data`.
- Consumer переподключается при потере AMQP-соединения и делает `basic_ack` только
  после успешной записи в БД (at-least-once доставка).
