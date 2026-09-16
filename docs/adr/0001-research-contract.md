# ADR-0001: Initial research contracts

Status: Accepted

## Context

The initial repository has no implementation. A shared runner must preserve
broker differences, failed experiments and independently variable workload axes.

## Decision

Use Go for orchestration and Python for derivation. Version 1 scenarios are
strict YAML objects validated with embedded JSON Schema plus semantic checks.
Broker packages implement a transport-neutral driver interface; deployment and
resource collectors are separate. Each invocation allocates exclusive run
folders, writes a provisional manifest before deployment, and seals a final
manifest with raw-file SHA256 inventory. Completed runs are never reopened.

Use a fixed absolute-deadline open-loop scheduler with a bounded queue per
producer. Missed queue admissions are recorded, never silently deferred into
an unbounded queue. Delivery latency starts at the intended send deadline,
using the runner's monotonic process clock; publish-operation latency starts at
actual invocation. Histograms retain HDR buckets; scheduling lag is separate.
A failed publish can be ambiguous, so delivery loss is measured relative to
attempted unique messages; accepted and delivered are independent counters.

Raw metric streams use versioned JSONL (portable, append-only while active).
Analysis exports Parquet and CSV as derived artifacts. JSONL is chosen over a
Go Parquet dependency for the first implementation; raw data remains complete.
Correctness uses disjoint sequence intervals per producer/subscriber, with an
explicit interval limit: exceeding it invalidates the run rather than silently
losing accounting precision. Fan-out counts expected copies per subscriber.

A single-channel broadcast application is the common baseline. Kafka uses a
separate consumer group per independent subscriber; broker resources are not
claimed equivalent. Unsupported topology/guarantee combinations are rejected.
Replicated profiles are rejected until multi-node deployment is implemented.

## Alternatives

Closed-loop generation hides stalls. Per-message maps grow with message count.
A common nominal acknowledgement level obscures protocol guarantees. All are
rejected. Cross-host one-way latency is outside this single-process design.

## Consequences and compatibility

Schemas, histogram encoding and metric definitions are versioned. Initial
contracts have no prior datasets to migrate. JSONL may be larger than Parquet.
Fragmented interval accounting can saturate and must be reported as invalid.
Scientific comparisons require identical controlled variables and documented
profile semantics, not an aggregate performance score.

## Refined controls

Density pins active producers/subscribers while scaling idle transport clients,
to keep both ingress and fan-out copies constant. Inactive clients have no
subscriptions or Kafka groups. Zero active count means all clients are active.
CRC32 covers the envelope and payload. Periodic reconnect supports churn.

Replay requires the same clean source commit and locks. Requested image digests
override tags and are checked against RepoDigests. Artifact hashes are streamed.
