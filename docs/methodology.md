# Методология

## Вопросы и переменные

Основной вопрос — влияние плотности клиентов на задержку, ресурсы и доставку.
Второй вопрос — влияние интенсивности сообщений при фиксированных клиентах.
Не смешивайте эти факторы в одном выводе.

`connection_density` изменяет только общее число logical clients. В примерах
ровно один producer и один consumer активны; остальные держат транспортные
клиенты без подписок. Это сохраняет как ingress, так и delivery rate при
увеличении плотности. `mostly_idle` использует тот же принцип при rate=0.
`message_rate` изменяет только суммарный publish rate. `payload_size` изменяет
полный размер benchmark message. `fan_out` отдельно меняет число независимых
подписчиков и закономерно увеличивает число доставляемых копий.

Зависимые переменные: offered/accepted/delivered rates, распределения latency,
correctness, CPU/RSS/working set, сеть, диск, фактические TCP endpoints и ошибки.
Контролируемые переменные: host/kernel, CPU governor, swap, хранилище, версии
и digest, профиль, активная topology, длительности, seed и параметры клиента.

## Фазы

Runner записывает PREPARE → DEPLOY → WAIT_HEALTH → SETUP → CONNECT → WARMUP →
MEASURE → COOLDOWN → VALIDATE → COLLECT → TEARDOWN → FINALIZE.

Readiness определяется healthcheck и затем protocol setup. Подключения создаются
с абсолютными ramp deadlines. Ramp ограничивает верхний темп; медленные операции
могут его уменьшить. При timeout/ошибке CONNECT запуск завершается failed с
частичным числом клиентов. Kafka ожидает назначения partition группе.

WARMUP использует отдельную метку фазы в envelope; его доставки исключены из
измерительных correctness/latency. MEASURE имеет фиксированную длительность.
COOLDOWN прекращает отправку и позволяет принять отложенные сообщения.
`delivered_window` относится к MEASURE, `delivered` включает cooldown. Lost
означает недоставленные к завершению cooldown ожидаемые копии, а не доказательство
того, что сообщение никогда не появится позднее.

## Open loop и coordinated omission

Глобальный scheduler формирует абсолютные плановые времена независимо от ответа
брокера. Активные producers выбираются round-robin с seed-зависимым начальным
смещением. Каждый имеет ограниченную очередь и выполняет одну publish operation
за раз. Очередь не растёт неограниченно: при переполнении фиксируется queue_dropped.
Истёкшие в очереди задания не переносятся за measurement window.

Deadlines вычисляются по интегралу заданного rate, включая burst-периоды, без
накопления округления отдельных interarrival durations. `sent` увеличивается
непосредственно перед реальным Publish. Задание без подключённого producer
учитывается в `disconnected_arrivals` и `queue_dropped`. Точные границы MEASURE
сохраняются в workload manifest, отдельно от времени записи stage log.

Publish operation latency измеряется от вызова до результата клиента. Delivery
latency измеряется от **планового** времени отправки до обработки consumer и
включает scheduling lag, producer queueing и настроенную consumer delay. Это
разные величины. Используется монотонное время одного Go process; wall clock
нужен только для UTC шкал. Cross-host one-way latency не поддерживается.

HDR сохраняет три значащие цифры в микросекундах; значения ниже 1 us округляются
до 1 us. Сохраняются buckets и snapshot, а также sparse оконные delivery buckets.
Неточные/переполненные гистограммы делают run invalid. Латентность потерянных или
непредложенных сообщений не выдумывается: графики всегда читаются вместе с
потерями и queue_dropped. Это не универсальная коррекция цензурированных данных.

## Границы генератора

`generator_saturated` выставляется, если sent/requested ниже `1-rate_tolerance`.
Это консервативная граница всего offering path: ограничение может находиться в
scheduler, CPU, serial publish, client library либо broker backpressure.
Флаг запрещает трактовать run как чистое измерение broker capacity. Атрибуция
требует scheduling-lag histogram, CPU/RSS/sockets и отдельной калибровки.
`broker_saturated` остаётся null: framework не делает причинный вывод по одному
числу. Burst requested count рассчитывается с учётом активных burst интервалов.

## Корректность и валидность

Используются per-producer/per-subscriber интервалы sequence numbers. Для competing
consumer delivery общий tracker на группу. Duplicate не считается полезной новой
копией; reorder интерпретируется только для независимых подписчиков. Глобальный
лимит интервалов ограничивает память; его превышение сохраняется как invalid.
FAST publish success может означать только client write completion: это явно
указано в manifest. Lost считается от попыток sent, а не от недоказанного приёма
брокером. Failure publish может иметь неопределённый исход.

Analysis всегда исключает failed, self-test, повреждённые/неполные артефакты и
неверную schema. По умолчанию также исключаются saturated offering path,
ошибка correctness accounting, отсутствие digest и недостаточные resource samples.
`--include-invalid` включает только помеченные completed measurements; failed и
checksum mismatch он не разрешает. Потери/duplicates сами по себе не скрываются:
это зависимые переменные исследования. Сохраняется `excluded.json` с причинами.

## Повторения и статистика

Exploratory examples задают 3 повторения; final-density задаёт 5, warmup 60 s,
measurement 300 s. Это значения YAML, не ограничения кода. Увеличивайте длительность
и число повторов по pilot runs. Smoke — только correctness, не научные результаты.

Группировка требует одинаковых scenario, source commit/dirty, image digest,
configuration hash и характеристик host. Разные режимы и размеры нагрузки не
смешиваются. Считаются mean, median, min, max, sample standard deviation.
Динамическая CPU frequency и used swap не входят в ключ повторений; модель CPU,
governor, размер RAM и swap configuration входят. Dirty/unknown source считается
invalid; `--include-invalid` разрешает показать его отдельно, но не объединяет
разные runs с неизвестными локальными изменениями.
Случайные имена/MAC/kernel IDs создаваемых Compose bridge/veth нормализуются
только в ключе группировки. Состав сети, принадлежность peers к bridge, MTU и
обычные NIC сохраняются как controls; raw inventory остаётся неизменным.
Политика описана в [ADR-0004](adr/0004-network-repetition-identity.md).
CI95 для среднего строится percentile bootstrap на уровне запусков: 10000 выборок,
seed=42. При одном повторе CI/stddev отсутствуют. При малом n bootstrap CI не
доказывает стабильность; показывайте individual runs. Percentiles сначала считаются
для каждого run; их mean не является percentile объединённого потока. p99.9
публикуется только при >=10000 observations (ожидаемо >=10 в верхней 0.1% области).

Recovery throughput — первый из трёх последовательных samples, каждый не ниже
90% среднего delivered rate до fault. Это показатель наблюдаемого восстановления,
а не момент окончания protocol reconnect. Если baseline нулевой или recovery не
наблюдался, значение null. Событие хранит собственную длительность separately.

## Сравнимость

FAST — broker-native ориентированный режим, DURABLE — более сильные нативные
гарантии. Ни один общий label не устанавливает эквивалентность fsync, replication,
session, consumer ack или queue/partition. Смотрите [brokers.md](brokers.md).
Формулируйте выводы только для конкретных workload, профиля, версий и оборудования.
Не используйте shared CI performance values как исследовательские результаты.

## Архивная baseline srv0

Для публикации данных октября 2026 года применяется дополнительная проверка
среды: явный disk resource alarm в broker log исключает запуск из основной
сравнительной агрегации, сохраняя его показатели в отдельном наборе наблюдений.
Это не автоматическое исключение из-за потерь. Правило, cohort и ограничения
описаны в [ADR-0005](adr/0005-srv0-archival-baseline.md) и
[отчёте baseline](../reports/baseline-srv0-20261109/README.md).
