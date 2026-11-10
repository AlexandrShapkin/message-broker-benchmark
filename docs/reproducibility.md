# Воспроизведение

## Подготовка

1. Сохраните исходники отдельным Git commit и проверьте clean tree. Framework
   не создаёт commits и не отправляет код автоматически.
2. Установите pinned зависимости: `make bootstrap build`.
3. Выполните `bench doctor`, `make test lint validate`, `make smoke`.
4. При доступе к registry выполните `.venv/bin/python scripts/lock_images.py`.
   Для уже скачанных Docker images доступен `--local-images`; он проверяет RepoDigest
   соответствующего repository, не подменяя его локальным image ID.
   Изменения versions.lock/Compose сохраните commit перед финальным исследованием.
5. Зафиксируйте YAML, seed, порядок matrix, timing, профиль и условия host.

Run сохраняет broker configuration, Compose, dependency locks, effective client
settings, scenario hash, Git commit/dirty, hardware/kernel, timing и workload.
RepoDigest хранится отдельно от image tag/ID. Неизвестный digest не выдумывается:
его отсутствие перед SETUP делает run failed.

## Повтор run

Перенесите весь неизменённый raw directory. На исходном либо явно задокументированном
новом оборудовании:

```bash
git checkout <manifest.source.commit>
make bootstrap build
export PATH="$PWD/bin:$PATH"
bench doctor
bench reproduce /path/to/saved/run
```

Строгий replay требует recorded clean source commit, валидирует manifest schema,
проверяет artifact SHA-256, обязательное присутствие sealed scenario и всех
deployment inputs. Сохранённые Compose, broker configs и lock files должны
совпадать с checkout; проверяется также общий configuration hash. Пути артефактов,
включая symbolic links, не могут выходить за архив. Проверки выполняются до
изменения состояния SUT. Он использует сохранённый scenario и immutable digest.
Результат получает новый run ID и собственную topology. Если scenario задаёт
несколько repetitions, выполняются все они.

Failed run тоже можно повторить, если он содержит все reproduction inputs и
зафиксированный digest. Незавершённый run и неполный архив replay отклоняет.

Эквивалентный явный запуск после проверки данных:

```bash
bench run --scenario /path/to/saved/run/scenario.yaml --image-digest sha256:<recorded-digest>
```

Один commit не воспроизводит dirty changes. Для exploratory dirty runs нужен
отдельно сохранённый checkout без credentials; строгий replay их отклоняет.
Для финального исследования используйте clean source commit.

## Повтор analysis

```bash
.venv/bin/python -m broker_analysis --results /path/to/archive --output analysis/output
```

Docker, работающий брокер, временные файлы и stdout runner не нужны. Schema берётся
из того же checkout. Проверяются manifest, hashes, scenario и consistency counters.
Таблицы/графики можно пересоздать; raw data изменять нельзя. `report.json` содержит
manifest hashes и статистический метод, `excluded.json` — причины исключения.

Архивируйте source commit, locks, YAML, полный raw directory и container images.
Digest гарантирует идентичность, но не вечную доступность образа в registry.
Сохраните также условия host: повтор на другом железе воспроизводит процедуру,
но не обязан давать те же численные значения.

## Восстановление после удаления рабочего checkout

Основная baseline и дополнительная acceptance-история сохранены в versioned
GitHub Releases. Оригинальный снимок srv0 также имеет удалённую копию.
[Пошаговая инструкция](restore.md) описывает чистый clone, проверку SHA-256,
получение raw data и source bundles без доступа к прежнему локальному диску.
