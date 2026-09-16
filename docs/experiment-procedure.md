# Порядок исследования

1. Выделите Linux host с контролируемой фоновой нагрузкой. Зафиксируйте hardware,
   kernel, governor, swap, FD limits, disk и network. Doctor не меняет настройки.
2. Выполните README setup, `make test lint validate`, затем `make smoke`. Каждый
   настоящий брокер должен стартовать, доставить данные и пройти analysis.
3. Проверьте manifests: completed, digest, configuration и profile; проверьте
   generator saturation, correctness counters и покрытие raw resource samples.
4. Проведите self-test generator. Увеличивайте idle clients и rate независимо;
   сохраняйте failed и saturated calibration runs.
5. Проведите pilot runs, выберите warmup, measurement, cooldown, ramp, operation
   timeout; затем зафиксируйте параметры перед основной серией.
6. Density сохраняет active clients и rate. Message-rate сохраняет общее и активное
   число клиентов. Fan-out — отдельный эксперимент.
7. Выполните минимум 3 exploratory либо 5 final repetitions. Для temporal drift
   используйте разные сохранённые порядки точек; не меняйте контролируемые параметры.
8. Failed runs исследуйте через failure_stage/reason, logs и partial measurements.
   Исправления порождают новый run, а не ручную замену failed на completed.
9. Запустите analysis, изучите excluded.json, latency, throughput, correctness,
   generator CPU/lag, missing values и sampling gaps.
10. Публикуйте individual repetitions, statistics/CI, workload, профиль, версии,
    оборудование и limitations. Не составляйте единый performance score.
11. Перенесите raw run в другой каталог и повторите analysis. Выполните reproduce
    из записанного clean commit, убедитесь в новом run ID.
12. Архивируйте source, locks, images и raw datasets отдельно от Git-кода.

```bash
bench matrix --scenario scenarios/research/final-density.yaml --broker nats
bench matrix --scenario scenarios/research/final-density.yaml --broker mosquitto
bench matrix --scenario scenarios/research/final-density.yaml --broker rabbitmq
bench matrix --scenario scenarios/research/final-density.yaml --broker kafka
make analyze
```

Эти команды не означают, что 50000-client эксперименты уже проверены на данном
host. Сначала должны пройти pilot runs, generator calibration и полный smoke.
