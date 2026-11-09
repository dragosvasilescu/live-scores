# ADR 0001 — Ingestion: Azure Event Hubs, partitioned by `matchId`

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

Match events arrive in spiky bursts: a goal sets off a flurry, and a Saturday 3pm kickoff starts many matches at once. Order matters within a match (a goal, then its disallowal) but not across matches. Two readers with different needs consume the same stream: a live scoreboard that wants low latency, and a highlights/stats consumer that replays a whole match's history later, on its own schedule. Clients retry (duplicates) and send malformed payloads.

## Decision

Use **Azure Event Hubs** (Standard, 8 partitions, auto-inflate 1→10 TUs, 7-day retention plus Capture to Blob). Producers send each event with **partition key = `matchId`**. Each reader gets its own **consumer group** (`scoreboard`, `highlights`) and its own checkpoints.

Why it fits:

- **Per-match order, parallelism across matches.** One match always maps to one partition, and a partition is an ordered log read by one processor at a time. That gives exactly the ordering the brief needs and no more. Different matches run in parallel across partitions.
- **Spiky load.** Event Hubs is a buffer: producers append at ingress speed and consumers catch up. A kickoff burst becomes a short lag, not back-pressure on clients. Auto-inflate covers the peak without paying for it all week.
- **Replay is built in.** The log is retained regardless of who has read it. The highlights reader rewinds to the start with its own consumer group and never affects the scoreboard. Capture keeps history beyond retention. The scoreboard can also be rebuilt from scratch by resetting its checkpoints.
- **Malformed and duplicate events.** The log accepts any bytes. Validation happens in the consumer, which routes bad events to a `rejected-events` container with a reason, and dedupes on `eventId` (see ADR 0002). Delivery is at-least-once, so the consumer must be idempotent in any case.

Locally this runs on the **Event Hubs emulator** (with Azurite for its metadata), using the same SDK and connection model as Azure.

## Alternatives rejected

- **Service Bus (queue/topic with sessions = matchId).** Sessions give per-match FIFO, and Service Bus adds a built-in dead-letter queue and duplicate detection. But messages are consumed and deleted. To replay a match for highlights we'd need a second subscription holding everything indefinitely, or a separate event store. *It would win* if each event were a command needing per-message settlement, retries and dead-lettering (e.g. "issue a refund"), or if traffic were low and replay were not a requirement.
- **Event Grid.** Push-based fan-out with no ordering guarantee and no replay. It also has no local emulator. *It would win* for reacting to discrete notifications ("match finished → send push notification"), where order and history don't matter.
- **Storage Queues.** Cheap and simple, but no ordering, no partitioning and no replay. *It would win* for a low-volume work queue where order is irrelevant.
- **HTTP API that writes directly to the store.** Fewest moving parts, but bursts hit the database directly, clients block on our writes, and there's no history to replay. *It would win* for a prototype with one reader and steady load.
- **Kafka (self-managed or Confluent).** Same log semantics. Event Hubs also exposes a Kafka endpoint, so we keep that option without running brokers ourselves. *It would win* in a shop already running Kafka, or one needing compaction or long retention at a lower cost.

## Consequences

- Partition count caps consumer parallelism and can't be changed on Standard tier. 8 is plenty for a league slate. More matches only means more keys per partition.
- Event Hubs doesn't dedupe or validate. The consumer owns both (ADR 0002).
- Events without a parseable `matchId` can't be keyed, so they land on any partition. That's harmless because they are always rejected.
