# Message Broker Research Framework

Единый Go runner и Python analysis для воспроизводимых экспериментов с NATS,
Mosquitto, RabbitMQ и Kafka. Источники истины — YAML scenario, manifest,
конфигурации, JSONL measurements и HDR distributions. Таблицы и PNG/SVG производны.

Реальные Docker smoke-запуски всех четырёх брокеров проверяют доставку, raw data,
manifest, ресурсы и построение таблиц/PNG/SVG. Текущие проверки и доказательства
собраны в [TASKS.md](TASKS.md). Перед финальным исследованием выполните
[процедуру проверки](docs/experiment-procedure.md) и учтите [ограничения](docs/limitations.md).

## Быстрый запуск

Требуются Linux, Go 1.27, Python 3.14 с venv, Docker Engine 25+ и Compose 2.20.2+, доступ
к registry. Docker должен быть доступен текущему пользователю. Framework не
меняет OS configuration, swap, governor или ulimit.

```bash
# Клонируйте этот repository и перейдите в его каталог.
make bootstrap build
export PATH="$PWD/bin:$PATH"
bench doctor
make test lint validate
make smoke
```

`make smoke` запускает настоящие короткие эксперименты для всех четырёх брокеров,
проверяет доставку и вызывает analysis. Ошибка даёт ненулевой exit code; заглушки
не подменяют real smoke. Shared CI используется только для correctness.
Smoke-таблицы и графики находятся в `analysis/smoke-output/<run_id>/`.
Проверка разрешает dirty checkout только для инфраструктурного smoke; насыщение
генератора, недостающие ресурсы, потери, дубликаты и ошибки отклоняются.

```bash
bench smoke --broker nats
bench run --scenario scenarios/smoke/nats.yaml
bench run --scenario scenarios/research/many_to_one.yaml --broker rabbitmq
bench matrix --scenario scenarios/research/connection_density.yaml --broker mosquitto
make analyze
make plots
bench summarize results/<run_id>
```

`run` выполняет repetitions; `matrix` разворачивает YAML axis/values. Density
изменяет logical clients при постоянных active clients и publish/delivery rate.
Message-rate matrix сохраняет clients. Fan-out исследуется отдельно.

## Сохранённые данные

```text
results/<unique-run-id>/
  manifest.partial.json      # evidence до deployment; остаётся при crash
  manifest.json              # final manifest + SHA256 inventory
  scenario.yaml
  sut-config/                # Compose, broker config, locks, effective settings
  logs/runner.log, broker.log
  metrics/*.jsonl            # generator, container, broker, events, optional operations
  latency/*.hdr.json         # publish, delivery, scheduling_lag, connect
  correctness/summary.json
```

Файлы создаются эксклюзивно; завершённые runs не открываются для записи. Блокировка
deployment исключает одновременное использование одинаковых broker ports. `clean`
удаляет состояние только Compose test instance; `reuse` сохраняет том. Результаты
не удаляются. Большие datasets игнорируются Git.

Analysis пишет отдельно в `analysis/output/`: runs/comparison/timeline в JSON,
CSV, Parquet, report.json, excluded.json, PNG/SVG. Failed/invalid runs исключаются
с объяснением. При отсутствии валидных данных analysis возвращает ошибку.

```bash
.venv/bin/python -m broker_analysis --results /path/to/raw-runs --output analysis/output
.venv/bin/python -m broker_analysis --run results/<run_id> --output analysis/output
```

`--include-invalid` показывает flagged completed measurements с меткой invalid;
он не разрешает failed, checksum mismatch или synthetic self-tests.
Dirty/unknown source исключается по умолчанию. Для проверки exploratory данных
можно явно добавить `--include-invalid`; перед научным сбором нужен clean commit.

## Дополнительные команды

```bash
bench provision --broker kafka
bench cleanup --broker kafka
bench self-test --clients 1000 --rate 100000 --seconds 10
bench reproduce results/<run_id>
.venv/bin/python scripts/lock_images.py
```

Self-test сохраняет local TCP/envelope measurements с меткой synthetic. Он не
является broker result. `reproduce` проверяет clean source commit, locks и hashes,
затем запускает сохранённый scenario с записанным digest в новый каталог.

Версии tags и SHA256 digests всех четырёх образов закреплены в `versions.lock.yaml`
и Compose. `lock_images.py` обновляет digests при доступном registry. Если registry
доступен только Docker daemon, сначала выполните `docker pull` закреплённых tags,
затем `.venv/bin/python scripts/lock_images.py --local-images`.
Runner требует реальный RepoDigest до SETUP. Digest
можно задать через `sut.image_digest` или `--image-digest sha256:...`.

## Документация и проверки

FAST/DURABLE задают broker-specific гарантии и не означают их эквивалентность.
Неподдерживаемые capabilities отклоняются до deployment.

- [Методология](docs/methodology.md): переменные, open loop, validity, statistics.
- [Брокеры](docs/brokers.md): гарантии и connection models.
- [Сценарии](docs/scenarios.md) и [определения метрик](docs/metrics.md).
- [Воспроизведение](docs/reproducibility.md) и [процедура](docs/experiment-procedure.md).
- [TASKS.md](TASKS.md): проверки и незавершённые пункты.

```bash
make test                 # Go + Python
make lint                 # gofmt check, go vet, Ruff, mypy
make validate             # YAML + capabilities
BENCH_INTEGRATION=1 go test ./tests/integration -count=1 -v
make recovery            # FAST/DURABLE, idle, reconnect, restart; реальные Docker runs
```

Recovery suite проверяет FAST/DURABLE, обычную доставку, idle transports,
reconnect storm и restart. Kafka idle длится 70 секунд, превышая 60-секундный
client idle timeout. `make recovery` сохраняет архивы в `results/contracts/`
и отключает Go test cache. При прямом вызове теста `BENCH_TEST_RESULTS` задаёт
каталог архивов; без этой переменной Go использует временный каталог теста.

Новый broker добавляется через factory, capabilities и deployment; runner,
raw storage и analysis не копируются. Смотрите [ADR](docs/adr/0001-research-contract.md).

## Базовые результаты srv0

[Русский отчёт, таблицы, графики и архив исходных данных](reports/baseline-srv0-20261109/README.md)
содержит измерения октября 2026 года и контракт сравнения с будущими брокерами.
