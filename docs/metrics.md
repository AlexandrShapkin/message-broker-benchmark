# Метрики и исходные данные

Raw sources: scenario.yaml, manifest.json, sut-config, logs, metrics/*.jsonl,
latency/*.hdr.json, correctness/summary.json. Все final artifacts имеют SHA-256
inventory. JSONL выбран как переносимый raw format; Parquet/CSV — производные.

| Поле | Определение |
|---|---|
| requested_messages/rate | интеграл настроенного open-loop schedule, включая burst |
| scheduled | реально выполненные callback scheduler |
| sent / offered_rate | publish invocations в MEASURE; rate = sent / configured seconds |
| producer_accepted | успешное завершение client Publish согласно профилю |
| delivered_window | валидные доставленные copies, обработанные до конца MEASURE |
| delivered | валидные copies фазы MEASURE, включая cooldown и duplicates |
| unique_delivered | новые sequence numbers по subscriber/group |
| consumer_acked | успешные explicit ack/commit вызовы; 0 при auto/no ack |
| lost | max(0, sent × expected copies − unique_delivered), bounded by cooldown |
| duplicate | повтор sequence в том же delivery accounting domain |
| unexpected | некорректный token/envelope, размер, неактивный producer или неотправленный sequence |
| reordered | позднее меньшее sequence для независимого subscriber |
| errors | ошибки publish/receive/ack; включая неоднозначный исход |
| disconnects | controlled client closes во время fault; не полный native disconnect counter |
| reconnections | успешные controlled client recreations |
| queue_dropped | arrivals, отвергнутые bounded queue или истёкшие до отправки |
| disconnected_arrivals | подмножество queue_dropped: задания без доступного producer; не входят в sent |

Envelope: magic/version, 128-bit run-id hash, producer ID, uint64 sequence,
monotonic deadline offset, phase, CRC32 и deterministic padding. Полный run_id находится
в manifest. Размер включает 64-byte minimum envelope, не только user payload.

Histogram files содержат HDR snapshot + distribution `{From,To,Count}` в us,
unit, encoding, schema version и count. Analysis использует верхнюю границу bucket
для percentiles в ms. `generator.jsonl.delivery_latency` хранит sparse histogram
за очередное sampling window для latency-versus-time.

Generator `/proc/self`: CPU ticks (CLK_TCK сохранён в host), RSS, threads, FD,
sockets, read_bytes/write_bytes. `/proc/net/dev` — **host namespace**, не attribution
network конкретному generator process. Не выдавайте его за exclusive process bytes.

Broker raw `/proc/<broker_pid>/status`, `/proc/<broker_pid>/io`, `/proc/net/dev`, tcp/tcp6 и cgroup v2
cpu.stat/memory.current/memory.stat/io.stat сохранены в broker-proc.jsonl. Это
RSS/threads, bytes/packets, disk bytes/operations, CPU/throttling и memory working
set (memory.current − inactive_file). Container должен иметь shell/proc; отсутствие
поля сохраняется как unavailable, не как ноль. I/O суммируется по устройствам
cgroup; PID I/O используется для совместимости со старыми архивами. Выбор процесса
сохраняется в `broker_pid`: у RabbitMQ PID1 является launcher, поэтому используется
`beam.smp`. Docker stats/inspect добавляют
working set и OOM state. Эти subprocess collectors сами потребляют ресурсы.

Physical connection count: established (`01`) server-side TCP endpoints, local
port брокера; management endpoints исключены. В manifest это sample перед teardown;
в таблице connections — maximum samples внутри MEASURE, connections_mean — mean.
Это не число logical objects и не доказательство постоянного соединения каждого
client. Healthcheck может кратковременно добавить endpoint.

CPU% нормирован на одно ядро, поэтому может превышать 100%. Resource summary
использует samples внутри MEASURE; peak — максимум наблюдений, не гарантия
регистрации короткого spike. Network/disk bytes — разности cumulative counters
между samples; reset создаёт новый epoch. Недостаточное покрытие делает run invalid.
RSS/client и RSS/connection имеют разные знаменатели. Useful MB/s пока использует
полный benchmark envelope, считая только unique copies внутри MEASURE. Accepted rate использует
producer_accepted_window; producer_accepted сохраняет также поздние подтверждения.

NATS `/varz`: connections, bytes/messages, memory/CPU, slow consumers. RabbitMQ
`/api/overview`: connection/channel/queue totals, backlog, publish/delivery/ack
statistics. Kafka JMX и MQTT `$SYS` пока не подключены: resource/protocol socket
collection для них общая. Внутренние counters разных brokers не эквивалентны.

Производные CSV записываются в UTF-8 с окончаниями строк LF; пустые поля означают
отсутствующее значение. Исходные JSONL/HDR при экспорте не меняются.
