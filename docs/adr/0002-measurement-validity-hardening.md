# ADR-0002: Scheduling, idle transports and analysis validity

Status: Accepted

## Context

The completion audit found accumulated scheduler rounding, skipped bursts at low
base rates, counts recorded before real Publish invocations, CPU frequency in
repetition identity, and incomplete manifests accepted by a shallow schema.
Kafka's client library also reaps idle sockets; an idle logical client alone is
not evidence of a maintained connection.

## Decision

Compute each open-loop arrival by inverting the integrated configured rate,
relative to the phase origin. An arrival rejected while disconnected is a dropped
arrival, not a sent message. Recheck the measurement deadline after acquiring the
producer slot. Record the exact measurement start/end in the manifest.

Keep inactive Kafka transports alive with explicit Metadata requests every ten
seconds and a sixty-second client idle timeout. These requests are control
traffic, excluded from application message counters but included in resource
measurements. Settings disclose this policy; physical endpoints are still
measured independently. Native consumer group heartbeats retain active consumers.

Validate completed broker manifests for experiment/software identity and workload
counters. Failed runs may have incomplete identity but must retain failure stage
and reason. Self-test manifests remain explicitly synthetic. Analysis checks
publish/delivery distributions and conservation of delivered copies, duplicates
and loss. Missing resource dimensions invalidate a run; unavailable CPU is null.

Group repetitions by stable hardware/configuration controls. Exclude live CPU
frequency and used swap from identity. Dirty/unknown source states are invalid
by default and never aggregated across different runs, even with include-invalid.

## Consequences and research impact

Burst runs and disconnected-arrival counts produced by earlier code require a
rerun; old raw data must not be edited. Tighter validation can reject incomplete
version-1 fixtures or archives. Existing valid version-1 files retain their
encoding, with new optional measurement timestamps. Analysis falls back to the
MEASURE log timestamp for older archives. Kafka idle control traffic is an
explicit broker-specific comparison condition, not equivalent to MQTT or NATS
heartbeats. Real Docker smoke and long idle/recovery tests are still required.
