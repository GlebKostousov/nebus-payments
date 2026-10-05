# Процессинг платежей

Сервис принимает запрос на оплату, проводит его через эмулятор платёжного шлюза и сообщает результат на webhook.

Платёж не проводится внутри HTTP-запроса. API сохраняет платёж и задание в одной транзакции и сразу отвечает `202`. Дальше один consumer забирает задание из RabbitMQ, вызывает шлюз, записывает статус и отправляет уведомление.

## Что нужно для запуска

Docker Compose v2. Команды из корня репозитория.

Скопируйте настройки.

Linux и macOS:

```bash
cp .env.example .env
```

Windows, PowerShell:

```powershell
Copy-Item .env.example .env
```

```bash
docker compose build
docker compose up -d --wait
```

При старте контейнер API применяет миграции и только потом открывает порт. Повторный запуск пропускает уже применённые версии.

Поднимаются `postgres`, `rabbitmq`, `api` и `consumer`. API слушает `127.0.0.1:8000`. Панель RabbitMQ — `http://127.0.0.1:15672`.

Логин базы берётся из `POSTGRES_USER` и `POSTGRES_PASSWORD`, логин брокера — из `RABBITMQ_USER` и `RABBITMQ_PASSWORD`. Если том базы уже создан, смена пароля в `.env` сама не применится: Postgres хранит пароль в данных тома. После `docker compose down -v` новый пароль заработает, старые данные пропадут.

Ключи в `.env.example` демонстрационные. Для настоящего стенда их нужно заменить.

`docker compose down` данные не удаляет. `docker compose down -v` удаляет тома вместе с базой.

## Создать платёж

`POST /api/v1/payments`

Нужны заголовки `X-API-Key` и `Idempotency-Key`. Сумма передаётся строкой, не числом: так в запрос не попадает двоичный float.

```bash
curl -i http://127.0.0.1:8000/api/v1/payments \
  -H 'X-API-Key: local-demo-api-key-change-before-deploy-123456' \
  -H 'Idempotency-Key: order-42-payment-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Оплата заказа 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook:8081/hooks/flaky"
  }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "6f63c0c7-9fbe-4722-8386-9ad8ba14c399",
  "status": "pending",
  "created_at": "2026-10-05T12:00:00Z"
}
```

`payment_id` и `created_at` генерирует сервис. `pending` в этой квитанции значит «запрос принят», а не текущий статус оплаты.

| Поле | Что передавать |
|---|---|
| `amount` | Строка больше нуля, до двух знаков после запятой. `"1500"` и `"1500.00"` — одна и та же сумма |
| `currency` | `RUB`, `USD` или `EUR` |
| `description` | Текст от 1 до 1024 символов |
| `metadata` | Объект JSON, по умолчанию `{}`, не больше 8 КиБ |
| `webhook_url` | Адрес из разрешённого списка origin |
| `Idempotency-Key` | Один заголовок, 1–128 символов, без пробелов |

Тот же ключ и то же содержимое возвращают первую квитанцию. Тот же ключ с другим телом — `409`. Без ключа или с неверным ключом — `401`, тело при этом не читается.

## Узнать состояние

`GET /api/v1/payments/{payment_id}`

```bash
curl http://127.0.0.1:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: local-demo-api-key-change-before-deploy-123456'
```

В ответе есть сумма, валюта, описание, metadata, адрес webhook, даты, `status` и состояние обработки.

`status` — итог оплаты: `pending`, `succeeded`, `failed`. Оплаченный платёж остаётся `succeeded`, даже если webhook ещё не доставлен. Если шлюз так и не ответил, статус не превращается в выдуманный `failed`.

Рядом лежит `processing_state`: `ready`, `running`, `completed`, `dead_lettered`. Неизвестный платёж — `404`.

`GET /health/ready` проверяет базу. Все маршруты, включая его и `/openapi.json`, закрыты тем же API-ключом. Swagger выключен.

## Как идёт обработка

1. API пишет платёж и строку outbox в одной транзакции.
2. Отдельный цикл публикует готовые строки в очередь `payments.new`. Пока RabbitMQ недоступен, задание остаётся в PostgreSQL и уходит позже.
3. Consumer вызывает эмулятор шлюза: пауза 2–5 секунд, около 90% успешных исходов и 10% отказов. Повтор того же платежа даёт тот же исход, а не новое случайное решение.
4. Статус сохраняется, затем уходит webhook. Если оплата уже записана, следующая попытка шлюз больше не вызывает.
5. Сообщение подтверждаем только после того, как решение записано в базу.

Ошибка доставки повторяется до трёх раз. После первой пауза около 2 секунд, после второй около 4. Срок хранится в `outbox.available_at`, отдельный Redis не нужен. Третья неудача закрывает автоматические попытки и кладёт событие в `payments.dlq`.

Повреждённое сообщение, которое нельзя разобрать, тоже попадает в DLQ: повтор его не починит.

## Webhook

Тело уведомления сохраняется вместе с результатом и при повторах не меняется. Меняются только время и подпись.

```text
Webhook-Id: <event_id>
Webhook-Timestamp: <unix_seconds>
Webhook-Signature: v1=<hex HMAC-SHA256>
```

Подпись считается от `timestamp + "." + тело` секретом `WEBHOOK_SIGNING_SECRET`. Успех — любой ответ `2xx`.

Разрешённые адреса задаются в `WEBHOOK_ALLOWED_ORIGINS`. В обычном режиме это HTTPS без логина в URL и без частных IP. Локальный HTTP для demo включается флагом `WEBHOOK_ALLOW_DEVELOPMENT`.

## Пример с повторами

Получатель webhook запускается отдельно:

```bash
docker compose --profile demo up -d webhook
docker compose exec -T api python -m scripts.smoke \
  --api http://api:8000 --receiver http://webhook:8081
```

Скрипт создаёт платёж и ждёт конца сценария. Адрес `http://webhook:8081/hooks/flaky` дважды отвечает `503`, затем `204`. В итоге три попытки и одно принятое событие.

Другие адреса того же получателя: `/hooks/success` и `/hooks/fail`. Список попыток — `GET /events` с тем же API-ключом. Это стенд в памяти, не боевой получатель.

## Проверки

Нужен Python 3.13 и [uv](https://docs.astral.sh/uv/). Тесты не стартуют без `TEST=1`, чтобы их нельзя было случайно запустить против рабочей базы.

```bash
uv sync
TEST=1 uv run pytest tests/unit
```

В PowerShell: `$env:TEST="1"; uv run pytest tests/unit`.

Интеграционные тесты поднимают свои PostgreSQL и RabbitMQ, не рабочую базу проекта:

```bash
docker compose -p nebus-tests -f compose.test.yaml up \
  --build --abort-on-container-exit --exit-code-from tests
docker compose -p nebus-tests -f compose.test.yaml down -v
```

Имя тестовой базы должно заканчиваться на `_test`. Этот прогон считает покрытие пакета `app` и падает, если оно ниже 80%.

## Стек

FastAPI, Pydantic v2, SQLAlchemy 2 (async), PostgreSQL, RabbitMQ через FastStream, Alembic, Docker Compose.
