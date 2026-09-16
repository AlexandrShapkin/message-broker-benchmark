# ADR-0003: Live smoke evidence and scalable raw readers

Status: Accepted

## Context

Real Docker tests exposed missing disk aggregates for brokers that drop process
privileges, although cgroup I/O was present in the raw archive. A development
checkout also needs full smoke verification without admitting its measurements
as clean-source scientific results. High-density TCP dumps must not cause
quadratic collector allocations or require loading a whole raw stream in Python.

## Decision

- Prefer cgroup `io.stat` across all devices over PID1 I/O; retain process counters
  as a compatibility fallback. An empty readable kernel counter means zero;
  an unavailable counter remains unknown. Keep the exact source in the archive.
- Select the real broker process by the driver's recorded `process_name` and
  lowest matching PID. RabbitMQ's PID1 is a shell launcher, not the BEAM VM.
  Preserve the selected PID and actual proc paths; reject old launcher RSS.
- Run native health probes every second during startup and once per 24 hours
  after readiness. Repeated JVM/Erlang diagnostic processes otherwise distort
  container CPU significantly. Controlled restart begins a new startup period.
  See [Compose healthcheck](https://docs.docker.com/reference/compose-file/services/#healthcheck).
- Collect proc sections with a linear buffer and stream JSONL records during
  analysis. Derived scalar timelines may remain in memory.
- Include the protocol port in driver settings. Generic socket counting and
  analysis consume that port. Optional native collectors live outside the runner.
- Track contiguous attempted sequence prefixes, one integer per producer;
  reject a valid-looking envelope outside that prefix or from an inactive producer.
- `bench smoke` verifies delivery, saturation, resource coverage, checksums and
  derived artifacts. Its dedicated analysis accepts a dirty development source
  only as infrastructure evidence. Ordinary analysis still rejects dirty sources.
- Digest locking can inspect already-pulled Docker RepoDigests with
  `--local-images`. Never use an image ID as a repository digest.

## Consequences

Old archives with cgroup counters can be reanalyzed without mutation. Previously
unknown disk aggregates become available. New ports require driver metadata,
not modifications to runner or analysis. Changes to envelope admission semantics
require fresh measurements. Smoke output is separate from research reports.
