# ADR 0002 — Current-state store: Azure Table Storage, a history table and a status table

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

The live scoreboard reads each match's current state (score, scorers, cards) with low latency, for one match or for all live matches. Only the consumer writes, and one partition processor owns a match at a time (ADR 0001). Updates must be idempotent: a duplicate or replayed `GOAL` must not turn 2-0 into 3-0. The data is small: tens of matches per slate, a few hundred events each.

## Decision

Use **Azure Table Storage** with two tables:

| Table | PartitionKey | RowKey | Holds |
|---|---|---|---|
| `matchevents` (history) | `matchId` | `eventId` | every valid event, one row each, plus its Event Hubs sequence number |
| `matchstatus` (status) | `matchId` | `status` | score, minute, event count, last event time |

For each event the consumer:

1. **Inserts it into history.** The key `(matchId, eventId)` is unique, so a second insert of the same event fails with 409, and that failure marks it as a duplicate. This one rule covers client retries, full replays and Event Hubs redelivery after a crash.
2. **Rebuilds the status from the match's history** (one partition query) and upserts it. The status is a pure function of history, so the two can't disagree. It is rebuilt even for a duplicate: if the consumer crashed between steps 1 and 2, the redelivered event repairs the status.

Reads:

- `/matches` scans `matchstatus`, which is small: one row per match.
- `/matches/{id}` is a point read on status plus one partition query on history for scorers, cards and subs.
- `/matches/{id}/events` is the match's deduplicated history in stream order. The highlights reader can also replay raw events from Event Hubs with its own consumer group (ADR 0001).

Ingestion counters sit in a third table (`ingestionstats`), one row per Event Hubs partition, so each row has a single writer. Locally everything runs on **Azurite**.

**Assumption:** a retry re-sends the same `eventId`, which is how the brief and the generator define a duplicate.

## Alternatives rejected

- **One row per match holding the score and the list of applied eventIds.** That is one write per event, and the dedupe and the score are atomic. But it needs ETag concurrency and retry loops, and the row grows with the match. Two tables are simpler to explain and test, and they give us the history for free.
- **Cosmos DB (NoSQL).** Fast point reads, a unique key on `eventId`, and a change feed that could push updates to clients. It costs more (RU provisioning) and the emulator is heavier locally. *It would win* with a global audience (multi-region reads), richer queries, or a change feed driving SignalR updates.
- **Azure Cache for Redis.** Lowest read latency, but it's a cache, so it would sit in front of a durable store, not replace it. *It would win* at very high read fan-out, as a read-through cache in front of `matchstatus`.
- **Azure SQL.** A unique constraint plus a transaction would make history and status atomic. It's more operational weight than a key-value read model needs. *It would win* if the scoreboard had to join relational data (fixtures, standings, players).

## Consequences

- An event costs four storage calls: insert, history query, status upsert, counter update. That's cheap at a few hundred events per second, but it's the first thing to optimise under heavier load (batch per match, or keep the status in memory per owned partition).
- History and status are two writes, not one transaction, because Table transactions only cover one partition of one table. The rebuild-from-history rule makes that safe: status is never more than one redelivery away from correct.
- If partition ownership moves mid-event, two processors could briefly race on one match's status. Both rebuild from history, so the next event for that match corrects it.
- Counters are at-least-once: a crash before checkpointing re-counts redelivered events as duplicates. Scores are never affected.
- "All live matches" is a table scan, which is fine for tens of matches. At scale we'd add a `live` partition or move to Cosmos DB.
