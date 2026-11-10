# Восстановление без прежней локальной копии

Репозиторий GitHub и два versioned Release достаточны для чтения результатов,
восстановления сохранённых данных и пересчёта baseline. Доступ к ThinkPad и srv0
не требуется. Benchmark на srv0 удалён; новые измерения при восстановлении
автоматически не запускаются.

## Чистый checkout

```bash
git clone https://github.com/AlexandrShapkin/message-broker-benchmark.git
cd message-broker-benchmark
```

Отчёт, CSV/JSON/Parquet основной серии, ключевые PNG/SVG и код анализа находятся
в Git. Для их просмотра скачивать гигабайтные raw data не нужно.
Локальные `.venv`, `bin/`, `.cache` и настройки редактора можно пересоздать;
SSH keys, credential stores и настройки личных инструментов в архивы не входят.

## Основная baseline srv0

Release: [baseline-srv0-20261109-v1](https://github.com/AlexandrShapkin/message-broker-benchmark/releases/tag/baseline-srv0-20261109-v1).
Он содержит raw data, inventories, образы брокеров и полные производные таблицы.

```bash
mkdir -p preservation/restore/srv0
gh release download baseline-srv0-20261109-v1 \
  --repo AlexandrShapkin/message-broker-benchmark --dir preservation/restore/srv0
(cd preservation/restore/srv0 && sha256sum -c SHA256SUMS)
cat preservation/restore/srv0/srv0-data-v1.tar.gz.part-* | \
  python3 scripts/verify_baseline_dataset.py \
    --inventory preservation/restore/srv0/published-inventory.jsonl \
    --run-directories preservation/restore/srv0/run-directories.txt
```

После установки Python >=3.11, `zstd` и pinned зависимостей пересчёт выглядит так:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r analysis/requirements.lock
cat preservation/restore/srv0/srv0-data-v1.tar.gz.part-* > preservation/restore/srv0/dataset.tar.gz
PYTHONPATH=analysis/src .venv/bin/python scripts/baseline_archive.py \
  preservation/restore/srv0/dataset.tar.gz preservation/restore/srv0/published-inventory.jsonl \
  reports/regenerated-srv0 --run-directories preservation/restore/srv0/run-directories.txt
MPLCONFIGDIR="$PWD/.cache/matplotlib" PYTHONPATH=analysis/src \
  .venv/bin/python scripts/render_baseline.py reports/regenerated-srv0
```

Docker нужен для будущих запусков брокеров, но не для пересчёта архива.
Готовые подробные metadata/timelines доступны без пересчёта в
`analysis-tables-v1.tar.gz`. Полная семантика, гарантии, ограничения и единицы —
в [отчёте](../reports/baseline-srv0-20261109/README.md) и
[контракте сравнения](../reports/baseline-srv0-20261109/COMPARISON.md).

## Дополнительное сохранение перед удалением локальной копии

Release: [local-retirement-v1](https://github.com/AlexandrShapkin/message-broker-benchmark/releases/tag/local-retirement-v1).

| Материал | Содержимое и назначение |
|---|---|
| `local-acceptance-history-v1.tar.gz` | 3943 файла из `results/`, `analysis/output/`, `analysis/smoke-output/`: старые smoke, synthetic, acceptance, failed и журналы проверок |
| `local-history-inventory.jsonl` | Размер и SHA-256 каждого файла локальной истории; копия также внутри архива |
| `local-history-summary.json` | 120 completed и 10 failed итоговых манифестов; классификация как infrastructure acceptance |
| `srv0-full-snapshot-v1.tar.gz.part-*` | Полный оригинальный снимок двух каталогов srv0, включая ранее исключённые из компактного набора Git metadata, бинарники, зависимости и кэши |
| `srv0-full-snapshot-inventory.jsonl` | Исходный серверный inventory: 14453 обычных файла, 60215606432 байта |
| `snapshot-review.json`, `bundle-object-review.json`, `bundles-verification.txt` | Проверки безопасной публикации Git metadata и исторических source bundles |
| `SHA256SUMS` | Контрольные суммы всех assets этого Release, кроме самого checksum-файла |

Сентябрьская локальная история **не входит** в научную baseline srv0. Она
сохраняет доказательства работоспособности, включая неудачные попытки. Два
`source.bundle` содержат полные истории отдельных source snapshots: commits
`184739ec1a4eb99b754443700b1cab4e618fa546` и
`686560e82e10c6cfc0e1de95532e3967b488505b`. Эти commits можно восстановить из
bundle, даже если они не входят в основную GitHub-ветку.

```bash
mkdir -p preservation/restore/retirement
gh release download local-retirement-v1 \
  --repo AlexandrShapkin/message-broker-benchmark --dir preservation/restore/retirement
(cd preservation/restore/retirement && sha256sum -c SHA256SUMS)
tar -xzf preservation/restore/retirement/local-acceptance-history-v1.tar.gz -C .
git bundle verify results/final-acceptance-20260912T152826Z/source.bundle
git bundle verify results/replay-acceptance-20260912T151146Z/source.bundle
# Для отдельного исторического checkout:
# git clone results/final-acceptance-20260912T152826Z/source.bundle ../historical-acceptance
```

Полный снимок сервера распаковывайте в отдельный пустой каталог:

```bash
mkdir -p preservation/restore/original-srv0
cat preservation/restore/retirement/srv0-full-snapshot-v1.tar.gz.part-* | \
  tar -xzf - -C preservation/restore/original-srv0
```

Он сохраняет исходные байты для аудита. Старые `.venv` и бинарники могут содержать
пути прежнего host; для исполнения используйте новый checkout и pinned dependencies.
Инвентарь описывает обычные файлы; symlinks и пустые каталоги также сохранены самим tar.

## Проверка независимости от локального диска

[Журнал передачи](../reports/local-retirement-v1/README.md) фиксирует проверки
скачанных архивов, source bundles и восстановления из чистого GitHub clone.
Все незаменимые материалы имеют удалённую копию. Оставшиеся локальные копии
архивов, распаковки, downloaded files и execution logs являются дубликатами либо
производными. Нужные журналы проверки включены в Git/Release; системные кэши
для восстановления не требуются.

Для удаления прежнего checkout отдельного действия здесь нет: инструкция
проверяет возможность восстановления, а сам каталог остаётся до решения владельца.
