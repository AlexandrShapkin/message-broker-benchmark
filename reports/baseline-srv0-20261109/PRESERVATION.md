# Архивирование и воспроизведение

Версия набора: [baseline-srv0-20261109-v1](https://github.com/AlexandrShapkin/message-broker-benchmark/releases/tag/baseline-srv0-20261109-v1).
Исходные измерения относятся к октябрю 2026, дата снимка — 2026-11-09 UTC.
Снимок сделан без запуска новых бенчмарков и без остановки экспериментов:
benchmark processes/containers отсутствовали; старые partial manifests не
считались доказательством продолжающегося эксперимента.

Сохранены оба каталога: `message-broker-benchmark` и `benchmark-plan`, включая
raw results, YAML, configs, HDR, логи, failed/incomplete, очереди и checkpoint
analysis. Серверный inventory и полный оригинальный снимок дополнительно
сохранены в [Release local-retirement-v1](https://github.com/AlexandrShapkin/message-broker-benchmark/releases/tag/local-retirement-v1);
локальный архив ThinkPad больше не является единственной копией.
Компактный архив основной baseline исключает `.git`, `.venv`, бинарники и кэши.
Дополнительный полный снимок сохраняет их после проверки безопасной публикации. Каждый пропуск
перечислен в `publication-omissions.json`; исходные measurements не редактировались.
Пустые каталоги попыток представлены отдельным `run-directories.txt`, содержащим
все 290 имён, в том числе два каталога без файлов.

Файлы Release:

| Артефакт | Назначение |
|---|---|
| `srv0-data-v1.tar.gz.part-*` | Последовательные части единого gzip/tar; перед чтением объединить в порядке имён |
| `published-inventory.jsonl` | SHA-256 и исходный byte size каждого опубликованного файла; тот же inventory внутри tar как `INVENTORY.jsonl` |
| `source-inventory.jsonl` | Серверный inventory всех 14453 обычных файлов снимка, до исключения зависимостей |
| `run-directories.txt` | Все каталоги попыток, включая пустые |
| `publication-omissions.json` | Исключённые зависимости/кэши/локальная Git metadata; не пропуски raw data |
| `broker-images.tar.gz` | Docker/OCI export четырёх pinned images; проверены SHA-256 всех 53 blobs |
| `analysis-tables-v1.tar.gz` | Полные производные таблицы, metadata audit и timelines, включая CSV/JSON/Parquet |
| `SHA256SUMS` | Контрольные суммы всех перечисленных Release assets, кроме самого checksum-файла |

Часть `broker-proc.jsonl` ранее заменена на `.zst` при аварийном освобождении
диска 12 октября. Оригинальный журнал операции и восстановитель сохранены в
корне архивного checkout. Analysis распаковывает их во временный каталог и
сверяет **исходный plaintext hash** из sealed manifest. Исходные `.zst` и даже
лишний исторический `.zst.zst` сохранены как найдено. Это не новая нормализация raw data.

Проверка секретов: опубликованные файлы проверены на private-key blocks,
GitHub/AWS/OpenAI token patterns; проверены конфигурации и планировщики.
`guest:guest` в RabbitMQ — явно обозначенная несекретная loopback-only fixture.
Личные SSH configs, agent credential stores и домашние каталоги инструментов
не включались. Сканер полного приватного снимка отметил два файла внутри `.venv`; оба исключены
вместе с зависимостями. На опубликованных файлах совпадений этих patterns нет.
Необходимых redactions измерений не обнаружено. Как и любой
pattern scan, такая проверка не является доказательством обнаружения всех
возможных форм секрета; найденные credentials нельзя добавлять при расширении архива.

Дополнительно проверены schema всех 575 файлов манифестов: 574 валидны; один
итоговый JSON пуст/повреждён и относится к incomplete попытке.
[Реестр проверки](manifest-validation.json) сохраняет hash, тип файла и статус.

## Загрузка и проверка

Из checkout опубликованного commit:

```bash
mkdir -p data/srv0-v1
# GitHub CLI должен быть авторизован для репозитория, если он закрыт.
gh release download baseline-srv0-20261109-v1 \
  --repo AlexandrShapkin/message-broker-benchmark --dir data/srv0-v1
(cd data/srv0-v1 && sha256sum -c SHA256SUMS)
cat data/srv0-v1/srv0-data-v1.tar.gz.part-* | \
  python3 scripts/verify_baseline_dataset.py \
    --inventory data/srv0-v1/published-inventory.jsonl \
    --run-directories data/srv0-v1/run-directories.txt
```

Последняя команда проверяет каждый опубликованный файл, встроенный inventory,
отсутствие лишних/повторных путей и список пустых попыток. Простого сообщения
об успешном upload недостаточно. Итог фактической проверки скачанной версии —
[remote-verification.json](remote-verification.json); первоначальная сверка
полного снимка с сервером — [verification.json](verification.json).

## Полный пересчёт

Нужны Python >=3.11, зависимости `analysis/requirements.lock`, CLI `zstd`
и временное место под самый большой отдельный run, не все 60 ГБ одновременно.
Команды ниже создают только производные данные; бенчмарки не запускаются.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r analysis/requirements.lock
cat data/srv0-v1/srv0-data-v1.tar.gz.part-* > data/srv0-v1/dataset.tar.gz
PYTHONPATH=analysis/src .venv/bin/python scripts/baseline_archive.py \
  data/srv0-v1/dataset.tar.gz data/srv0-v1/published-inventory.jsonl \
  reports/regenerated-srv0 --run-directories data/srv0-v1/run-directories.txt
PYTHONPATH=analysis/src .venv/bin/python scripts/render_baseline.py reports/regenerated-srv0
```

`data/` — пример локального пути; скачанные гигабайтные файлы не коммитить.
Рабочая папка `preservation/` уже исключена из Git; можно использовать её вместо
`data/`. `baseline_archive.py` проверяет manifests/schema, hashes и counters через
существующий `run_summary`, сохраняет все исключения, применяет ADR-0005 и выводит
таблицы. `render_baseline.py` строит читаемые таблицы и PNG 240 dpi/SVG на русском.
Полный текст интерпретации — редакционный отчёт; числовые таблицы и графики
пересоздаются командами выше. `finalize_baseline.py` позволяет повторно применить
классификацию к уже вычисленным таблицам с проверенными копиями manifests/logs,
не перечитывая огромные proc streams; для обычного пересчёта он не нужен.

Перед удалением srv0 проверяются commit на GitHub, скачанные части архива,
каждый файл по inventory и сохранённые images. Завершение очистки фиксируется
отдельным commit после её фактической проверки.
