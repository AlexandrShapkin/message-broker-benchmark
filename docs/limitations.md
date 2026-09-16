# Ограничения и статус проверки

Docker smoke подтверждает работу инфраструктуры на этом host, но не является
аттестацией произвольной 50000-client матрицы. Закреплены реальные RepoDigests всех
четырёх образов. Пилотные исследования больших плотностей нужны на целевом
оборудовании. Числа из unit fixtures и shared CI не являются broker results.

Текущая реализация имеет следующие явные границы:

- Один host/process генератора, один broker node, один application channel.
  Multi-node replication и cross-host latency не поддерживаются.
- MQTT и Kafka competing consumers не реализованы; validator отклоняет запрос.
- Docker exec/stats polling создаёт измерительный overhead. Точная частота зависит
  от host; sampling gaps и короткие spikes требуют проверки raw timestamps.
- RSS/threads/FD относятся к выбранному процессу брокера, CPU/I/O — к cgroup.
  Для RabbitMQ выбирается BEAM, для Kafka JVM. Сохранены process_name и реальный
  PID. Старые RabbitMQ archives с RSS launcher отклоняются. Cgroup v1 exact
  counters отсутствуют; CPU cgroup включает работу вспомогательных процессов.
- Healthchecks выполняются часто до readiness, затем раз в 24 часа; это исключает
  постоянный запуск диагностических JVM/BEAM внутри обычного measurement window.
  Для очень длинных runs редкий probe остаётся частью измерительного overhead.
- Host-network generator counters содержат сторонний трафик. Нет exclusive network
  attribution к process. Broker-side TCP count может включить protocol health probe.
- Native Kafka JMX и Mosquitto `$SYS` collectors ещё не подключены. Full native
  disconnect/reconnect counters не унифицированы; `disconnects` считает controlled
  closes. Не интерпретируйте 0 как доказательство отсутствия native disconnect.
- Reconnect поддерживает периодический churn через fault.repeat_seconds. Broker
  restart — один event на run. Retry параметры native clients фиксированы
  в effective settings, а не во всех случаях настраиваются YAML.
- FAST acknowledgement semantics различаются. MQTT DURABLE не означает per-message
  fsync; Kafka RF1 не обеспечивает отказоустойчивость к потере диска.
- Publish worker выполняет одну operation за раз. Недостаток concurrency может
  ограничить offering path; такой run flagged и исключается по умолчанию.
- Deadline latency относится к доставленным сообщениям. Для dropped arrivals нет
  выдуманного latency. Lost bounded by cooldown, failed publish outcome неоднозначен.
- Interval accounting ограничено по памяти и может стать invalid при фрагментации.
- Self-test проверяет локальные TCP pairs и envelope scheduling, а не максимальную
  ёмкость всех production client libraries. Максимум нужно находить серией запусков.
- Analysis grouping не объединяет разные host/configuration/digest. Bootstrap CI
  при малом числе повторов не является доказательством устойчивости результата.
  Нормализация временных сетевых идентификаторов ограничена стандартными Compose
  bridge/veth; custom naming требует проверки. Реальные изменения состава сети
  разделяют repetitions. Не запускайте сторонние Docker workloads во время исследования.
- Dirty source commit сам по себе не воспроизводит локальные изменения: перед
  финальным сбором данных сохраните исходники в отдельном commit и проверьте dirty.
  Analysis исключает dirty/unknown source по умолчанию; include-invalid показывает
  такие runs отдельно, без объединения неизвестных локальных изменений.
- SIGKILL/отключение питания могут оставить manifest.partial.json и неполный JSONL.
  Это незавершённый run; analysis не принимает его как completed. Файловая система
  не защищается от ручного изменения владельцем; checksums обнаруживают изменения.

Ограничения должны сопровождать публикацию результатов. Удалять их можно только
после реализации и соответствующей проверки, а не ради формального завершения.
