# ADR-0004: Network identity for repetition grouping

Status: Accepted

## Context

Live three-repetition matrices split identical configurations when Docker Compose
recreated a network. The raw `ip -j link` inventory includes generated bridge and
veth names, MAC addresses and kernel indices. These are instance identifiers,
not hardware changes. Conversely, dropping every virtual interface would conceal
changes in network configuration and bridge membership.

## Decision

Keep the full original network inventory in each immutable manifest. In derived
repetition identity only, canonicalize the generated `br-<12 hex digits>` bridges
used by these deployments and their namespace-linked `veth` peers. Remove their
generated names, MAC addresses, interface/peer indices and namespace IDs.

Retain each bridge with its own sorted list of peers. Preserve link counts, MTU,
queue settings, state and all other recorded controls. Preserve ordinary adapters
and custom-named bridges unchanged; interface listing order is not a factor.
Changing a physical NIC, MTU or bridge membership still separates repetitions.
This recognition policy is scoped to the repository's default Compose networking;
custom network naming needs explicit review before extending normalization.

Docker documents the [Compose default network](https://docs.docker.com/compose/how-tos/networking/)
and [configurable bridge interface names](https://docs.docker.com/engine/network/drivers/bridge/).
The generated identifiers above were also observed directly in the live archives.

## Consequences

Existing raw archives can be reanalyzed without mutation. Group IDs and aggregate
tables may change, so retain the analysis source revision with derived reports.
Real network topology changes still split groups. Concurrent infrastructure tests
are not controlled scientific experiments, even when their functional checks pass.
