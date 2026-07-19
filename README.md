# Apache Airflow + RabbitMQ — Event-Driven Demo

RabbitMQ используется как шина событий. **Отдельный сервис `rabbitmq-consumer`**
(вне Airflow) слушает очереди и по статичному маппингу `queue -> DAG` вызывает
Airflow REST API, запуская нужный DAG и передавая ему тело сообщения как `conf`.
DAG затем вызывает внешний REST-сервис (с его собственной авторизацией). Airflow
НЕ держит долгоживущих коннектов к RabbitMQ и не блокирует слоты воркеров.
Consumer не имеет коннекта к базе данных — его живость отдаётся через HTTP
health-эндпоинт.

## Поток данных

```
RabbitMQ (100 очередей)
   │  basic_consume (push), отдельный сервис
   ▼
rabbitmq-consumer  ── POST /api/v1/dags/{dag_id}/dagRuns (conf=payload) ──▶ Airflow
   │  restart: unless-stopped (self-healing вне Airflow)                         │
   │  GET /healthz (живость процесса, без БД)                                    ▼
   │                                                         DAG: PythonOperator
   │                                                           requests.post(внешний API)
   ▼                                                                             │
внешний REST-сервис                                                   (watchdog опрашивает /healthz)
```

## Сервисы (docker-compose.yml)

| Сервис              | Назначение                                                       | Порты          |
|---------------------|------------------------------------------------------------------|----------------|
| `postgres-airflow`  | Метаданные Airflow                                               | 5432 (внутр)   |
| `rabbitmq`          | Брокер (очереди) + management UI                                | 5672, 15672    |
| `redis`             | Celery broker для Airflow                                        | 6379 (внутр)   |
| `airflow-webserver` | Веб-UI + REST API (`/api/v1`)                                    | 8080           |
| `airflow-scheduler` | Планировщик DAG                                                  | —              |
| `airflow-worker`    | Celery-воркер, исполняет DAG-задачи                             | —              |
| `rabbitmq-consumer` | **Отдельный** сервис: слушает очереди, триггерит DAG через API  | 8081 (healthz) |
| `producer`          | Пример-приложение, отправляющее сообщения в очередь             | —              |

## Структура

```
docker-compose.yml
consumer/
  Dockerfile              # лёгкий образ (python:3.11-slim + pika/requests)
  consumer_service.py     # router: RabbitMQ -> Airflow REST API + HTTP /healthz (вне Airflow)
  queue_dag_map.json      # шаблон маппинга на 100 очередей (с комментариями)
dags/
  rabbitmq_external_api_dags.py  # шаблон DAG (заглушка POST к внешнему API) + генерация 100
  rabbitmq_watchdog.py           # монитор живости сервиса через HTTP /healthz (alert only)
producer/
  Dockerfile
  producer.py             # отправляет тестовое сообщение в очередь (env RABBITMQ_QUEUE)
airflow/Dockerfile        # кастомный образ Airflow
```

## Запуск

```bash
docker compose build
docker compose up -d
# unpause демо-DAG-ов (созданы файлом dags/rabbitmq_external_api_dags.py):
docker compose exec airflow-scheduler airflow dags unpause demo_orders
docker compose exec airflow-scheduler airflow dags unpause demo_signals
docker compose exec airflow-scheduler airflow dags unpause demo_alerts
# unpause watchdog:
docker compose exec airflow-scheduler airflow dags unpause rabbitmq_watchdog
# отправить тестовое сообщение в очередь (orders|signals|alerts):
docker compose run --rm -e RABBITMQ_QUEUE=orders producer
```

Сообщение из `orders` вызовет `POST .../dags/demo_orders/dagRuns` с `conf`,
DAG `demo_orders` запустится и выполнит заглушку внешнего вызова.

## Проверка

Очередь пуста после обработки:
```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
```
Conf у последнего run DAG-а (подтверждает доставку payload из очереди):
```bash
docker compose exec rabbitmq-consumer python -c "
import requests; s=requests.Session(); s.auth=('admin','admin')
print(s.get('http://airflow-webserver:8080/api/v1/dags/demo_orders/dagRuns/<run_id>').json()['conf'])
"
```
Живость consumer-сервиса (HTTP healthz, без БД):
```bash
curl -s http://localhost:8081/healthz
```

## Масштабирование на ~100 очередей

- `consumer/queue_dag_map.json` — статичный маппинг `queue -> dag_id` (шаблон на 100).
  Смонтируйте его в сервис и задайте `QUEUE_DAG_MAP=/app/queue_dag_map.json`.
- DAG-и генерируются той же фабрикой `make_dag()` (см. `dags/rabbitmq_external_api_dags.py`),
  читая тот же маппинг — одна очередь → один DAG.
- Один `rabbitmq-consumer` процесс держит 100 `basic_consume`. Для распределения
  нагрузки поднимите N реплик сервиса и разделите маппинг между ними
  (каждая реплика получает подмножество очередей).
- В k8s вместо `restart: unless-stopped` используйте ReplicaSet/Deployment —
  он даёт настоящий self-healing и горизонтальное масштабирование.

## Доступы

- Airflow UI / API: http://localhost:8080 — `admin` / `admin`
  (consumer использует эти же учётные данные для Basic Auth к REST API)
- RabbitMQ UI: http://localhost:15672 — `airflow` / `airflow`
- Consumer health: http://localhost:8081/healthz

## Заметки по архитектуре

- **Consumer вне Airflow.** Long-running процесс больше НЕ внутри воркера
  (это был антипаттерн: блокировал слот, умирал при рестарте воркера). Теперь
  это отдельный сервис с `restart: unless-stopped` (или ReplicaSet в k8s).
- **Без PostgreSQL для конвейера.** Consumer не пишет в БД; метаданные Airflow
  живут в `postgres-airflow`. Живость сервиса отдаётся через HTTP `/healthz`,
  а не через таблицу в БД.
- **At-least-once.** При сбое вызова Airflow API сообщение возвращается в очередь
  (`basic_nack(requeue=True)`), событие не теряется.
- **Watchdog** только алертит, если `GET /healthz` недоступен или отдаёт
  устаревший `age` (>60с); сам сервис перезапускается политикой restart,
  watchdog не триггерит его (он вне Airflow).
- **DAG — заглушка.** В `call_external_api` закомментирован реальный `requests.post`;
  авторизация внешнего сервиса выносится в Airflow Connection/Variable.
