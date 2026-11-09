# Live Football Scores

A live-scores service. Match events stream through **Azure Event Hubs**, a consumer folds them into per-match state in **Azure Table Storage**, and a small HTTP API serves the scoreboard and ingestion health. A second consumer group lets a highlights reader replay any match's full history on its own.

```
generator ─► Scores.Producer ─► Event Hubs "match-events" (8 partitions, key = matchId)
                                  ├─ cg "scoreboard" ─► Scores.Consumer ─► Table: matchevents, matchstatus, ingestionstats
                                  │                                     └► Blob:  rejected-events, checkpoints
                                  └─ cg "highlights" ─► Scores.Producer replay (full history, own pace)
                         Scores.Api ─► Table (read only):  /matches  /matches/{id}  /ingestion/health
```

Decisions: [ADR 0001: ingestion](docs/adr/0001-ingestion-event-hubs.md) · [ADR 0002: current-state store](docs/adr/0002-state-store-table-storage.md)

## Run it locally

Needs Docker and the .NET 10 SDK. No Azure subscription.

```bash
# 1. Local infra: Event Hubs emulator + Azurite
docker compose up -d                     # wait for http://localhost:5300/health

# 2. Consumer and API (separate terminals)
dotnet run --project src/Scores.Consumer
dotnet run --project src/Scores.Api --urls http://localhost:5240

# 3. Generate the hostile stream and publish it
dotnet run tools/event_generator.cs -- --count 1500 --seed 42 --out events.ndjson
dotnet run --project tools/Scores.Producer -- send events.ndjson          # add --delay-ms 20 for a "live" feed

# 4. Read
curl localhost:5240/matches              # all matches (summary)
curl localhost:5240/matches/<matchId>    # one match: score, scorers, cards, subs
curl localhost:5240/matches/<matchId>/events   # one match's full history, deduped, in order
curl localhost:5240/ingestion/health     # processed / duplicates / rejected (+ per partition)

# 5. Replay: send the same file again. Scores don't move; everything valid counts as a duplicate.
dotnet run --project tools/Scores.Producer -- send events.ndjson

# 6. Highlights: one match's full history via its own consumer group
dotnet run --project tools/Scores.Producer -- replay --match <matchId>
```

Rejected events are stored as JSON blobs (reason, detail, raw body) in the `rejected-events` container in Azurite. You can browse them with Azure Storage Explorer.

Tests: `dotnet test`. They use in-memory stores, so no Docker is needed.
IaC: `az bicep build --file infra/main.bicep && az bicep lint --file infra/main.bicep`.

### What a run shows (seed 42, 1500 events)

| | first send | after sending the same file again |
|---|---|---|
| processed | 1204 | 1204 |
| duplicates | 150 | 1504 |
| rejected | 146 | 292 |
| scoreboard | matches an independent count of distinct valid goals | byte-identical |

Rejected doubles on the resend because the malformed events really were delivered twice. Each delivery is rejected and stored on its own.

## How the tricky bits work

**Two tables.** `matchevents` is the history: one row per event, `PartitionKey = matchId`, `RowKey = eventId`. `matchstatus` is the scoreboard: one row per match with the score and minute. The whole flow is in [EventIngestor.cs](src/Scores.Domain/Ingestion/EventIngestor.cs):

1. Parse. A malformed event goes to the rejected sink with a reason.
2. Insert into history. Inserting the same `(matchId, eventId)` twice fails with 409, so that's a duplicate. This covers client retries, a full stream replay, and Event Hubs redelivery after a crash.
3. Rebuild the status from the match's history ([MatchStatus.FromHistory](src/Scores.Domain/State/MatchStatus.cs)) and save it. Status is a function of history, so it can't drift. A crash between steps 2 and 3 is repaired when the event is redelivered.

**Order.** The producer keys by `matchId`, so a match lives on one partition. Event Hubs keeps partition order, and one processor owns a partition at a time. History rows store the Event Hubs sequence number, so a match's history reads back in stream order.

**Malformed events.** [EventParser](src/Scores.Domain/Events/EventParser.cs) never throws. Each failure gets a typed reason (`UnparseablePayload`, `MissingField`, `InvalidField`, `UnknownType`, `GoalWithoutScorer`). Rejects go to Blob storage, named by partition and offset, so a redelivered reject overwrites its own blob. One bad event never blocks the good events around it.

**Failure handling.** Storage errors are retried with backoff and never skipped, because skipping would lose a goal. Each partition is checkpointed every 50 events and on shutdown. Anything after the last checkpoint is redelivered, and the history table turns it into duplicates.

## Assumptions

- **Event rate.** A league slate is about 10 concurrent matches. The burst case is a kickoff or goal flurry of hundreds of events per second, not millions. 8 partitions and 1→10 auto-inflate TUs cover that with plenty of headroom.
- **A duplicate is the same `eventId` sent again.** That's how the brief and the generator define a retry. A retry keeps its `eventId`; a different `eventId` is a different event. The generator's `seq` field is ignored, so events don't need one. A late event with a new `eventId` is applied, and `Minute` / `LastEventAt` take the maximum so it can't move the clock back.
- **Duplicate = ignore and count.** A duplicate isn't an error and isn't stored again. It increments `duplicates`.
- **Match status** is the score per team, the latest minute and a timestamp. Scorers, cards and subs come from the history. Teams are learned from the events (there's no fixtures feed).
- **Counters are at-least-once.** A crash between counting and checkpointing can count a few redelivered events again as duplicates. Scores are exact.
- **Locally**, connection strings point at the emulators. In Azure the apps use managed identity (`Storage:AccountName`, `EventHubs:FullyQualifiedNamespace`), and the Bicep disables SAS keys and shared-key access.

## Repo layout

| Path | What |
|---|---|
| `src/Scores.Domain` | Parsing, the ingestor (the whole flow), match status, and the Table/Blob stores |
| `src/Scores.Consumer` | Event Hubs processor host (`scoreboard` consumer group) |
| `src/Scores.Api` | Read endpoints |
| `tools/Scores.Producer` | `send` NDJSON → Event Hubs; `replay` one match via the `highlights` group |
| `tools/event_generator.cs` | The provided generator (copy of `requirements/`) |
| `tests/Scores.Tests` | Two tests: dedupe and malformed input |
| `infra/main.bicep` | Event Hubs, Storage, Container Apps, RBAC, Capture |
| `infra/local/` | Event Hubs emulator config (hub, partitions, consumer groups) |
| `.github/workflows/ci.yml` | Build, test, Bicep build + lint |

## With more time

- **Push, not poll:** stream state changes to clients over SignalR/WebSockets. The consumer already knows exactly when a match changes.
- **Corrections:** a `GOAL_DISALLOWED` event that references the goal's `eventId`, so a goal and its disallowal are both applied in order. `MatchStatus.FromHistory` would skip the disallowed goal.
- **Fewer round trips under burst:** each event is currently four storage calls (insert, history query, status save, counters). Batching per match, and keeping status in memory for owned partitions, would cut that to about one write per match per batch.
- **Live index:** a `status=live` partition, or Cosmos DB, so "all live matches" isn't a table scan as the history grows.
- **Rejected-event tooling:** an endpoint to list and inspect rejects, an alert on reject rate per producer, and a way to re-drive fixed events.
- **Deployment:** Dockerfiles, an image push and `az deployment group what-if`/`create` in the pipeline. Container Apps KEDA scaling on consumer lag. Pinned emulator image tags.
- **Integration test in CI** against the emulators (docker compose in the job), sending the generator stream twice and checking the scores.
