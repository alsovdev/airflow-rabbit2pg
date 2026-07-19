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
dags/rabbitmq_consumer_daemon.py  # DEPRECATED: DAG-обёртка, запускает consumer как long-running задачу
dags/rabbitmq_watchdog.py         # Watchdog DAG: проверяет heartbeat и авто-перезапускает consumer
producer/producer.py              # Пример producer-а
producer/Dockerfile
airflow/Dockerfile                # Кастомный образ Airflow
init-db/01_init.sql               # Создание таблицы messages
init-db/02_heartbeat.sql          # Создание таблицы consumer_heartbeat
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
он не планируется шедулером, а запускается вручную и работает постоянно, пока
жив процесс (consumer НЕ завершается при простое). Graceful shutdown по SIGTERM
(кнопка Clear / завершение run). Если consumer остановлен, перезапустите run
вручную — накопленные в очереди сообщения будут обработаны.

> **DEPRECATED**: long-running consumer внутри Airflow-воркера — антипаттерн
> (блокирует слот воркера, падает при рестарте воркера). Оставлено для демо;
> в проде consumer выносится в отдельный сервис вне Airflow.

## Watchdog (наблюдение и авто-перезапуск)

DAG `rabbitmq_watchdog` (`schedule="*/5 * * * *"`) раз в 5 минут проверяет
«живость» consumer-а по таблице `consumer_heartbeat`:

- Consumer пишет туда запись `last_seen` каждые `HEARTBEAT_INTERVAL` сек (15).
- Watchdog берёт `MAX(last_seen)`; если `NOW() - last_seen > WATCHDOG_THRESHOLD_SECONDS`
  (60), считает consumer мёртвым и **автоматически перезапускает** legacy-DAG
  `rabbitmq_consumer_daemon` через `airflow dags trigger`. Иначе — success.
- При неудаче trigger-а таск падает с `AirflowException` (видно в UI/алертах).

```bash
docker compose exec airflow-scheduler airflow dags unpause rabbitmq_watchdog
docker compose exec airflow-scheduler airflow dags trigger rabbitmq_watchdog   # ручная проверка
```

Смотреть heartbeat:

```bash
docker compose exec postgres-data psql -U datauser -d datadb -c "SELECT MAX(last_seen) FROM consumer_heartbeat;"
```

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
- Таблица `consumer_heartbeat` (init-db/02_heartbeat.sql) используется watchdog-ом
  для определения живости consumer-а вне Airflow API. Пороги настраиваются через
  env: `HEARTBEAT_INTERVAL`, `WATCHDOG_THRESHOLD_SECONDS`.
