        # kafka — Идемпотентность и обзор exactly-once

        Homework-шаблон для урока **l3_idempotency_eos_intro** (Идемпотентность и обзор exactly-once) на платформе Vibe Learn.

        ## Что делать

        Реализуй тест на дедупликацию для идемпотентного Kafka producer на Go.
Продьюсер пишет 100 сообщений с retries=10. В процессе отправки искусственно вызываются
сетевые сбои через iptables в docker-контейнере брокера.
Consumer читает все полученные сообщения и проверяет:
- При `enable.idempotence=true` — дублей нет (count уникальных = 100).
- При `enable.idempotence=false` — дубли присутствуют (count > 100).
CI assert: idempotent run → unique_count == 100; non-idempotent run → duplicate_count > 0.

## Контекст (из transfer-задачи урока)

Команда строит платёжный пайплайн: `order_events` → processor → `payment_events`.
Требование: каждое событие из `order_events` порождает ровно одно событие `payment_events`,
даже при сбоях (сетевые blip'ы, рестарты сервиса).

Команда поставила `enable.idempotence=true` на producer, который пишет в `payment_events`.
Они считают, что этого достаточно.

## Recap из урока

- `enable.idempotence=true` устраняет дубли при ретраях, но только в рамках **одной партиции и одной сессии продьюсера**. Рестарт приложения — новый PID, гарантия сбрасывается.
- Транзакции (`transactional.id` + `initTransactions` + `beginTransaction` + `commitTransaction`) дают **атомарную запись поперёк партиций и топиков** — это следующий уровень поверх idempotence.
- `isolation.level=read_committed` на консьюмере — обязательная третья часть EOS: без неё consumer видит сообщения из aborted-транзакций.
- Реальный end-to-end EOS = транзакционный продьюсер + `read_committed` + `sendOffsetsToTransaction` в той же транзакции. Убери любую ногу — EOS нет.
- Антипаттерн: «мы поставили idempotence, у нас exactly-once» — нет, это at-most-once-per-partition-per-session. Для production EOS нужен полный стек транзакций.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
