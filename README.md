# Message Broker Research

Reproducible framework for benchmarking message brokers under high client
density and controlled messaging workloads.

Supported research targets:

- Apache Kafka
- NATS
- RabbitMQ
- Eclipse Mosquitto

## Repository documentation

Start here:

1. `PROJECT.md` — purpose, scope and research model.
2. `AGENTS.md` — mandatory rules for coding agents.
3. `ROADMAP.md` — implementation milestones.
4. `TASKS.md` — current implementation state.
5. `DECISIONS.md` — architecture decision index.
6. `docs/methodology.md` — experimental methodology.

## Development

```bash
make bootstrap
make test
```

## Research workflow

```text
scenario
→ validation
→ deployment
→ workload
→ measurement
→ immutable raw results
→ analysis
→ plots/tables
```

Do not use generated charts as the only copy of experimental results.
