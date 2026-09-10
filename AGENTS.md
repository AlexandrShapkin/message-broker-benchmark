# AGENTS.md

## Purpose

This repository contains a reproducible research framework for benchmarking
message brokers under high client density and varying messaging workloads.

The repository is a research instrument, not a production messaging system.

The primary systems under test are:

- Apache Kafka
- NATS
- RabbitMQ
- Eclipse Mosquitto

Additional brokers may be added through the same driver abstraction.

The main research dimensions are:

- concurrent client density;
- message rate;
- payload size;
- producer/consumer topology;
- fan-out;
- slow consumers;
- reconnect storms;
- broker restart/failure behavior;
- CPU, RAM, network and disk resource consumption;
- latency distributions;
- delivery correctness.

Read `PROJECT.md` before making architectural changes.

Read `docs/methodology.md` before changing benchmark semantics.

Read `ROADMAP.md` and `TASKS.md` before starting new implementation work.

Read `DECISIONS.md` and `docs/adr/` before changing an existing architectural
decision.

---

## Core principles

The following rules are mandatory.

### 1. Research correctness is more important than implementation convenience

Do not change experiment semantics merely because another implementation is
easier.

Do not silently introduce assumptions.

Do not hide limitations.

Do not reinterpret incomparable broker features as equivalent.

### 2. Raw measurements are the source of truth

The following are source artifacts:

- scenario configuration;
- run manifest;
- exact SUT configuration;
- software versions;
- raw measurements;
- latency histograms;
- correctness counters;
- system metrics;
- logs needed to explain a run.

The following are derived artifacts:

- CSV summaries;
- aggregate tables;
- PNG;
- SVG;
- charts;
- written conclusions.

A chart that cannot be recreated from stored data is invalid.

### 3. Connection density and traffic intensity are separate variables

Do not conflate:

- number of logical clients;
- number of physical network connections;
- number of messages per second.

Connection-density experiments must be able to vary connection count while
holding traffic rate approximately constant.

Message-rate experiments must be able to vary traffic rate while keeping client
count fixed.

### 4. Logical clients and network connections are different metrics

Never label a logical client count as "connections" unless the actual network
connection count has been measured.

Broker/client-library connection pooling must be documented.

### 5. Broker-specific behavior must stay behind drivers

Core experiment runner code must not depend directly on:

- Kafka-specific APIs;
- NATS-specific APIs;
- AMQP-specific APIs;
- MQTT-specific APIs.

Use broker driver abstractions.

### 6. Unsupported capabilities must be explicit

Do not emulate a broker feature silently.

If a scenario requires unsupported semantics:

- reject the scenario; or
- explicitly mark the compatibility mode and document what is being emulated.

### 7. Reproducibility is mandatory

Every run must record enough information to reproduce it.

At minimum:

- run ID;
- scenario;
- scenario hash;
- Git commit;
- dirty working tree state;
- broker version;
- broker image digest;
- client-library version;
- configuration hash;
- hardware information;
- OS/kernel;
- timing parameters;
- random seed;
- requested workload;
- actual workload;
- errors and warnings.

### 8. Existing results are immutable

Never overwrite an existing run directory.

Never silently delete experiment results.

Never edit raw results after a run has completed.

Corrections belong in processing logic or a new run.

### 9. Failed experiments are data

A run that fails must still create a manifest.

Record:

- failure stage;
- failure reason;
- partial measurements where safe;
- warnings.

Do not convert failure into success to simplify downstream processing.

### 10. Load-generator saturation must be detected

A benchmark is invalid as a broker-capacity measurement if the load generator
became the bottleneck first.

Always collect resource metrics for the load generator.

Track:

- requested rate;
- actual offered rate;
- scheduling lag;
- CPU;
- RAM;
- sockets/connections.

### 11. Measure distributions, not only averages

Latency reporting must support at least:

- p50;
- p95;
- p99;
- p99.9 where statistically meaningful;
- maximum.

Preserve histogram/raw distribution data.

### 12. Avoid coordinated omission

Saturation experiments should use an open-loop workload model or another
explicitly justified technique that does not hide stalls by reducing request
generation during high latency.

### 13. Do not use performance numbers from shared CI as research results

CI may validate:

- compilation;
- tests;
- correctness;
- schema validity;
- smoke functionality.

Scientific performance measurements require a controlled environment and a
complete run manifest.

---

## Scope

The repository SHALL implement:

- experiment scenario model;
- scenario validation;
- broker driver abstraction;
- automated broker deployment;
- load generation;
- system metrics;
- broker-specific metrics where available;
- correctness validation;
- raw result storage;
- run manifests;
- statistical aggregation;
- plot generation;
- reproducibility tooling;
- smoke and integration tests.

The repository is NOT intended to implement:

- a new message broker;
- a generic production observability platform;
- Kubernetes infrastructure;
- a web GUI;
- a production SaaS benchmarking service.

Do not expand scope without an explicit decision.

---

## Development strategy

Work in small vertical increments.

Do not implement multiple large subsystems in one change.

Preferred pattern:

1. understand current repository state;
2. identify one bounded task;
3. state assumptions;
4. implement;
5. add or update tests;
6. run relevant checks;
7. update documentation;
8. update `TASKS.md`;
9. create an ADR if an architectural decision was introduced.

A task is incomplete until its relevant tests pass.

---

## Initial implementation order

Preferred vertical implementation:

1. repository skeleton;
2. scenario schema;
3. run manifest;
4. result model;
5. CLI;
6. first complete NATS vertical slice;
7. Mosquitto driver;
8. RabbitMQ driver;
9. Kafka driver;
10. host/system collectors;
11. research scenario matrix;
12. analysis pipeline;
13. failure scenarios;
14. generator validation and hardening.

Do not build abstractions for hypothetical future brokers before at least one
complete driver works end-to-end.

---

## Task size

Do not treat requests such as:

> implement the benchmark framework

as one coding task.

Break them into independently verifiable changes.

A good task should have:

- clear input;
- clear output;
- narrow affected area;
- explicit acceptance criteria;
- explicit tests.

Large changes should be decomposed before implementation.

---

## Required checks

Before considering a Go change complete, run the applicable commands:

```bash
go test ./...
go vet ./...
gofmt
```

Run repository lint targets if they exist.

Before considering Python analysis changes complete, run the configured:

```bash
pytest
```

and configured lint/type checks.

For schema changes, validate example scenarios.

For driver changes, run the corresponding broker smoke test when Docker is
available.

Do not suppress failing tests.

Do not delete tests simply because implementation changed.

---

## Documentation rules

Documentation is part of the implementation.

Update documentation when behavior changes.

Important documents:

- `PROJECT.md` — stable repository mission and scope;
- `ROADMAP.md` — implementation stages;
- `TASKS.md` — current executable backlog;
- `DECISIONS.md` — decision index;
- `docs/methodology.md` — scientific methodology;
- `docs/metrics.md` — metric definitions;
- `docs/brokers.md` — broker semantics/configuration;
- `docs/scenarios.md` — scenario semantics;
- `docs/reproducibility.md` — reproduction procedure;
- `docs/limitations.md` — known threats to validity;
- `docs/adr/` — architectural decisions.

Do not duplicate a large specification across several files.

Prefer references.

---

## Architecture decisions

Create an ADR when introducing or changing a decision that affects:

- scenario semantics;
- driver contracts;
- raw-data schema;
- manifest schema;
- latency measurement;
- workload generation model;
- storage format;
- broker comparison methodology;
- dependency boundaries.

Do not create ADRs for trivial implementation details.

---

## Performance work

Do not optimize based on intuition alone.

First:

1. establish correctness;
2. measure;
3. identify the bottleneck;
4. optimize;
5. measure again.

A performance optimization must not silently alter benchmark semantics.

---

## Dependencies

Prefer mature and maintained dependencies.

Before adding a major dependency:

- verify that it is necessary;
- check whether an existing dependency already solves the problem;
- isolate broker-specific dependencies inside broker-specific packages.

Pin versions through normal language/package lock mechanisms.

Pin SUT container versions.

Never use `latest` for research runs.

---

## Security

Never commit:

- passwords;
- API keys;
- private TLS keys;
- access tokens;
- credentials.

Test credentials must either be generated or clearly marked as non-secret local
fixtures.

Do not log credentials.

---

## Result handling

Large result sets must not be committed to normal Git history.

`results/` should normally be ignored except for small fixtures/examples.

Do not delete user-generated datasets.

---

## When something is unclear

If ambiguity affects scientific validity, protocol semantics, comparison
fairness, or result interpretation:

DO NOT silently choose an arbitrary interpretation.

Either:

- use an existing documented decision;
- create a proposed ADR;
- clearly mark the issue as unresolved.

For ordinary implementation details that do not affect semantics, choose the
simplest maintainable solution.

---

## Definition of done for a change

A change is complete when:

- required functionality works;
- relevant tests pass;
- no known regression is hidden;
- documentation reflects behavior;
- schemas/examples are updated if required;
- no temporary debug artifacts remain;
- `TASKS.md` reflects the new state.

Do not claim completion based only on code generation.
