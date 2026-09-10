# PROJECT.md

# Message Broker Research Framework

## Purpose

This project implements a reproducible experimental framework for comparing
message brokers under high client density and controlled messaging workloads.

Primary systems under test:

- Apache Kafka
- NATS
- RabbitMQ
- Eclipse Mosquitto

The framework should also allow additional message brokers to be integrated
through a common driver interface.

## Research questions

Primary question:

How does increasing the number of simultaneously connected clients affect
latency, throughput, resource consumption and stability of different message
brokers?

Secondary questions:

- How does increasing message rate affect the same systems independently of
  connection density?
- How does payload size affect system behavior?
- How does fan-out affect resource use?
- How do slow consumers affect backlog and latency?
- How do brokers behave during reconnect storms?
- How do brokers recover after controlled restart?
- What trade-offs are introduced by persistence and stronger delivery
  guarantees?

## Independent variables

Primary variables:

- number of logical clients;
- number of network connections;
- offered message rate;
- payload size;
- producer count;
- consumer count;
- topology;
- fan-out;
- acknowledgement/persistence profile;
- consumer processing rate;
- failure scenario.

## Dependent variables

Primary measurements:

- publish latency;
- end-to-end delivery latency;
- p50/p95/p99/p99.9;
- requested/offered/accepted/delivered throughput;
- CPU;
- RSS;
- network traffic;
- disk I/O;
- connection establishment success;
- disconnects;
- lost messages;
- duplicate messages;
- reordered messages where ordering is expected;
- recovery time.

## Systems under test

### Apache Kafka

A distributed partitioned log/message streaming system.

### NATS

Test Core NATS and JetStream only when the selected scenario requires the
corresponding semantics.

### RabbitMQ

AMQP-oriented message broker.

### Eclipse Mosquitto

MQTT broker used as the baseline MQTT implementation.

MQTT itself must never be described as the tested broker implementation.

## Comparison philosophy

Different brokers have different architecture and semantics.

The framework should compare equivalent application workloads as closely as
possible without pretending internal broker concepts are identical.

Any semantic mismatch must be documented.

## Main experiment families

- connection density;
- message-rate saturation;
- payload size;
- one producer to many subscribers;
- many producers to one consumer/group;
- many producers to many consumers;
- mostly idle clients;
- slow consumer;
- reconnect storm;
- broker restart.

## Reproducibility

Every run must preserve:

- scenario;
- manifest;
- versions;
- SUT configuration;
- raw measurements;
- correctness metrics.

Generated plots are not primary research data.

## Technical direction

Current baseline:

- Go — runner and load generation;
- Python — analysis;
- Docker Compose — SUT deployment;
- YAML — scenarios;
- JSON Schema — validation;
- Parquet / histogram files — raw measurement storage.

Technology choices may evolve, but research semantics must remain stable or be
versioned explicitly.

## Non-goals

This project does not implement a new broker.

It is not intended to become:

- a production benchmarking SaaS;
- Kubernetes operator;
- graphical management system;
- generic monitoring platform.

## Success criterion

The project is successful when a researcher can select a broker and scenario,
run an experiment, preserve all raw data and metadata, regenerate analysis and
later reproduce the same experiment without reconstructing undocumented state.
