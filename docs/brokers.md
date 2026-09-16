# Брокеры и профили

Все deployment одноузловые, порты опубликованы только на loopback. Брокеры
изолированы в `drivers/`, deployment — в `internal/deployment`, сбор ресурсов —
в `internal/systemmetrics`. CLI является composition root реестра драйверов.
Версии закреплены в `go.mod`, `go.sum`, `versions.lock.yaml` и Compose.

| Брокер | FAST | DURABLE | Подписчики |
|---|---|---|---|
| NATS 2.11.8 | Core NATS, Publish + server Flush, без сохранения сообщений | JetStream file stream, publish ack, manual AckSync, durable consumer | subject subscription на consumer; shared queue group для competing |
| Mosquitto 2.0.22 | MQTT 3.1.1, QoS0, clean session, non-retained | QoS1, persistent session, manual consumer Ack; broker autosave 1 s | независимая MQTT subscription |
| RabbitMQ 4.1.4 | classic non-durable queues, transient messages, без confirms, auto ack | durable classic queues, persistent messages, publisher confirms, manual ack | fanout exchange, отдельная очередь на subscriber; общая очередь для competing |
| Kafka 4.0.1 | один KRaft node, одна partition, RF1, leader ack, idempotence off | all-ISR ack при RF1, idempotent producer, synchronous offset commit | отдельная consumer group на subscriber |

REPLICATED отклоняется. Competing в MQTT/Kafka drivers этой версии отклоняется;
это ограничение реализации, а не утверждение об отсутствии такой возможности
в протоколе или брокере. Несколько application channels пока отклоняются.

Kafka всегда пишет log и использует page cache. Даже DURABLE/RF1 не означает
per-message fsync или устойчивость к потере диска. MQTT PUBACK также не означает
немедленный fsync; client store — memory, client process crash recovery не
реализован. NATS Flush в FAST подтверждает обработку соединения сервером, но не
доставку subscriber. FAST AMQP/MQTT success — client write completion, поэтому
`producer_accepted` нельзя без пояснения называть broker-confirmed rate.

Полные существенные настройки сохраняются в `sut-config/`: исходный Compose,
broker config, versions lock и `effective-settings.json`; дополнительные поля
SUT/identity находятся в final manifest. Kafka: linger 0, compression none,
batch max 33554432, fetch max 33554432, fetch wait 100 ms, retention 24 h.
NATS: file stream max 1 GiB/24 h, replicas 1, pending 65536 messages / 64 MiB.
RabbitMQ: prefetch 256, one connection/channel per client, heartbeat 10 s.
MQTT: inflight 20, broker queued messages 10000, keepalive 30 s, no retain.

Connection semantics: NATS/MQTT/AMQP создают отдельное TCP соединение на logical
client. Kafka client может открыть отдельные broker/coordinator connections;
число объектов не выдаётся за измеренное число соединений. Inactive consumers
создают транспортный клиент без subscription/group. Подключения protocol probes
и instrumentation могут кратковременно попадать в server-side endpoint samples.

Для неактивных Kafka transports и zero-rate producers driver выполняет Metadata
Ping раз в 10 s; client idle timeout закреплён на 60 s. Это сохраняет idle sockets
и инициирует восстановление после restart. Этот управляющий трафик входит в
CPU/network, но не в application message rate; политика сохранена в manifest.
Её нельзя считать семантически равной MQTT keepalive или NATS protocol ping.

Встроенное восстановление NATS и MQTT, повторное открытие закрытых AMQP clients и
native Kafka recovery скрыты внутри drivers. Controlled reconnect закрывает
выбранные clients, выдерживает downtime и параллельно создаёт новые. Для FAST
профилей сообщения за время разрыва могут потеряться; для persistent subscribers
возможны повторные доставки. Reuse сохраняет том брокера, но каждый новый run
использует новое имя topic/subject/queue. Старые ресурсы накапливаются до cleanup;
это нужно учитывать при сравнении clean и reuse.

API references: [nats.go](https://github.com/nats-io/nats.go),
[Paho Go](https://github.com/eclipse-paho/paho.mqtt.golang),
[AMQP client](https://pkg.go.dev/github.com/rabbitmq/amqp091-go@v1.10.0),
[franz-go](https://github.com/twmb/franz-go).
