# ROADMAP.md

# Implementation Roadmap

## R0 — Repository foundation

Deliver:

- repository structure;
- CLI skeleton;
- scenario model;
- JSON Schema;
- run manifest;
- immutable result layout;
- documentation structure;
- CI baseline.

Exit criteria:

- project builds;
- tests run;
- example scenario validates;
- empty/synthetic run manifest can be generated.

## R1 — First vertical slice: NATS

Deliver complete path:

scenario
→ deployment
→ producer/consumer
→ workload
→ measurement
→ correctness validation
→ result directory
→ analysis
→ plot

Exit criteria:

A NATS smoke scenario can be executed from a clean environment and produces a
valid result set.

## R2 — MQTT / Mosquitto

Add Mosquitto through the common broker driver model.

Exit criteria:

Same high-level scenario abstraction executes against NATS and Mosquitto.

## R3 — RabbitMQ

Add RabbitMQ support.

Exit criteria:

Common smoke scenarios pass.

## R4 — Kafka

Add Kafka support.

Explicitly document differences between logical clients and physical broker
connections.

Exit criteria:

Common smoke scenarios pass.

## R5 — Measurement infrastructure

Complete:

- host metrics;
- container/process metrics;
- generator health metrics;
- connection counts;
- latency histograms;
- correctness counters.

Exit criteria:

Generator bottleneck can be distinguished from SUT bottleneck.

## R6 — Research workloads

Implement scenario families:

- connection density;
- message-rate sweep;
- payload sweep;
- fan-out;
- many-to-one;
- many-to-many;
- mostly-idle;
- slow consumer;
- reconnect storm;
- broker restart.

Exit criteria:

Each scenario is configuration-driven and documented.

## R7 — Analysis

Implement:

- run discovery;
- validation;
- repetition aggregation;
- statistical summaries;
- comparison tables;
- plot generation;
- invalid-run handling.

Exit criteria:

Raw results can regenerate all final tables and plots.

## R8 — Research readiness

Complete:

- generator self-tests;
- reproducibility validation;
- documentation;
- experiment procedure;
- limitations;
- end-to-end smoke suite.

Exit criteria:

The framework is ready for execution of the final research matrix.
