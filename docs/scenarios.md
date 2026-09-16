# Сценарии

Schema v1: `schemas/scenario.schema.json`. Примеры YAML полностью задают параметры;
неизвестные поля, enum, broker, отрицательные значения, невалидные topology и
несовместимые capabilities отклоняются до deployment.

- `sut`: broker, профиль `fast|durable`, состояние `clean|reuse`.
- `clients.producers/consumers`: число logical clients, минимум один каждой роли.
- `clients.active_producers/active_consumers`: активные роли; 0 означает все.
  Неактивные клиенты поддерживают transport без публикации/подписки.
- `traffic.total_rate_messages_per_sec`: **суммарный** publish rate активных producers.
  Rate=0 — idle. `payload_bytes` — весь envelope с padding, минимум 64 bytes.
- `queue_capacity`: bounded pending deadlines на каждый активный producer.
- `consumer_delay_ms`: время обработки каждой доставки перед ack.
- `burst_multiplier`, `burst_every_seconds`, `burst_duration_seconds`: периодический
  burst, начинающийся с первой фазы периода; multiplier=1 отключает его.
- `topology`: `one_to_many`, `many_to_one`, `many_to_many`; один application channel.
  Delivery `broadcast` создаёт независимые копии; `competing` допустим для NATS/AMQP.
- `connection`: ramp clients/s, общий connect timeout, timeout одной операции.
- `timing`: warmup, measurement, cooldown в секундах.
- `collection`: resource interval 100–1000 ms, обязательные latency histograms,
  optional operation samples и global interval cap correctness.
- `fault`: offset от MEASURE, fraction выбираемых clients и downtime.
  repeat_seconds=0 — один event; положительное значение повторяет reconnect для
  continuous churn. Выбирается та же seed-зависимая группа клиентов.
- `matrix`: ось и список значений; пустая ось означает обычный run.

`bench run` выполняет repetitions, `bench matrix` — каждую точку и её repetitions.
Density axis `clients` задаёт чётное общее число; половина producers, половина
consumers, active counts остаются прежними. Rate axis не меняет clients; payload
axis не меняет rate. Fan-out axis меняет consumers. Порядок YAML сохраняется;
для контроля временного дрейфа создавайте отдельные YAML с рандомизированным
порядком точек и сохраняйте их как исходные данные.

`scenarios/research/` содержит все десять основных семейств и final-density.
Брокер переопределяется `--broker`, затем capabilities валидируются повторно.
Например:

```bash
bench validate scenarios/research/connection_density.yaml
bench matrix --scenario scenarios/research/connection_density.yaml --broker mosquitto
bench run --scenario scenarios/research/slow_consumer.yaml --broker rabbitmq
bench run --scenario scenarios/research/broker_restart.yaml --broker kafka
```

Для Kafka может потребоваться увеличить operation timeout, warmup и connect timeout
по данным pilot run. Ошибки не подавляются. Не меняйте настройки между repetitions
и не смешивайте такие результаты в одну группу.
