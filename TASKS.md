# Implementation status

Work proceeds through independently verified increments (ROADMAP.md).

- [x] R0: strict scenario/schema validation, CLI, immutable run store and manifests.
- [x] R1–R4: real deployment and delivery smoke for NATS, Mosquitto, RabbitMQ and Kafka.
- [x] R5: generator/container/proc collectors, HDR, saturation and cgroup I/O.
- [x] R6: configuration-driven workload and fault scenario library.
- [x] R7: raw validation, repetition statistics, tables, Parquet and PNG/SVG.
- [x] R8: full profile/recovery suite, clean-source replay, independent matrices
  and archive-only analysis acceptance completed within documented scope.

## Live acceptance audit — 2026-09-11–12

- [x] Docker Engine/Compose available; all four pinned images pulled.
- [x] All four real repository digests locked; local-image resolution tested.
- [x] FAST delivery smoke for every broker: 1000 publications and 10000 copies,
  zero losses, duplicates, unexpected messages and operation errors.
- [x] CLI smoke includes Python raw/schema/checksum/resource validation and plots.
- [x] Fixed unavailable process I/O by consuming archived cgroup device counters.
- [x] Fixed RabbitMQ RSS selection: collect BEAM instead of PID1 shell launcher.
- [x] Reduced repeated heavyweight native health probes after startup readiness.
- [x] Rejected unsent sequence numbers using constant-space producer prefixes.
- [x] Streamed raw JSONL input and removed quadratic proc-text concatenation.
- [x] Isolated failed-deployment unit fixtures from live deployment locks.
- [x] Calibrated 10000 local TCP pairs with requested 100000 envelopes/s for 5 s;
  archived as synthetic, not as a capacity bound or broker result.
- [x] Complete 32 FAST/DURABLE delivery/idle/reconnect/restart contracts across
  all four brokers; independently validate every raw archive. Two additional
  Kafka idle contracts pass with a 70 s measurement, beyond the client idle timeout.
- [x] Repeat final `make smoke`: all four brokers, zero delivery correctness
  failures, full resource coverage, four accepted archives and comparison plots.
  Output: `analysis/smoke-output/20260912T153226Z-nats-smoke-afeaf433d868dfe7/`.
- [x] Verify clean-source replay and archive-only regeneration: two real NATS
  runs, distinct IDs, unchanged original hashes, one repetition group, zero
  exclusions, 14 PNG/SVG pairs. Archive and source bundle:
  `results/replay-acceptance-20260912T151146Z/`.
- [x] Harden replay preflight: validate manifest, sealed scenario/configuration,
  current Compose/config equality and archive path containment; 15 regression cases.
- [x] Run final Go/Python checks and inspect the working-tree diff: Go tests/vet,
  gofmt, 30 Python tests, Ruff, mypy and all example schemas. Go race checks pass.
- [x] Fix live repetition splitting on generated Docker network IDs, preserving
  bridge membership and other network controls (ADR-0004; four regression tests).
- [x] Repeat generator self-test: 10000 TCP pairs, requested 100000 envelopes/s,
  5 s; synthetic archive `20260912T153402Z-generator-self-test-c6de8466892969e1`.
- [x] Finish isolated density/rate/payload matrix acceptance: 18 valid runs,
  six groups with three repetitions each, zero losses/duplicates/errors/saturation,
  independent axes, comparison tables/Parquet and 123 PNG/SVG pairs. Raw archive
  and clean source bundle: `results/final-acceptance-20260912T152826Z/`;
  report: `analysis/output/final-acceptance-20260912T152826Z/`.

Acceptance command logs are retained in `analysis/output/acceptance-audit/`.
Final `make analyze plots` also passes against the accumulated result tree;
failed, synthetic, dirty and otherwise invalid archives retain explicit exclusions.
The isolated source snapshots have local commits and Git bundles; the user's
working-tree history and existing raw datasets have not been rewritten.

Earlier failed/DEPLOY archives are preserved. HTTP 403 on direct registry requests
was resolved for daemon pulls; direct lock resolution can instead use pulled
RepoDigests via --local-images. Old RabbitMQ launcher RSS is invalid evidence;
corrected resource measurements require a new run. No raw archives are edited.

Large-client scientific matrices still require calibration on their target host.
No shared-host or synthetic measurements are presented as scientific conclusions.

## Архивная baseline srv0 — 2026-11-09 UTC

- [x] Сохранить все исходные файлы и образы, сверить серверный SHA-256 inventory.
- [x] Пересчитать per-run данные и строгие агрегаты; отделить auxiliary и disk-alarm observations.
- [x] Опубликовать русский отчёт, CSV/JSON/Parquet, PNG/SVG и воспроизводимые команды.
- [x] Проверить push и скачанный versioned Release перед удалением серверных данных.
- [x] Удалить исключительно benchmark resources srv0, проверить остатки и свободное место.
