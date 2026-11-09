# Technical Challenge — Live Football Scores Service

## Context

We want to see how you build *and* how you decide. This exercise has two small architecture decisions for you to make and defend, wrapped around a working vertical slice you build yourself. Use any tools you like, including AI — we do too. What matters is that **you own and understand everything you submit**. On Friday, you'll walk us through it and justify your choices.

Budget about **two evenings**. We'd far rather see the core done well, with the tricky bit handled thoughtfully, than something sprawling and half-finished. If you run short, leave the happy path working and note what you'd do next.

## The scenario

You're building a live-scores service for football matches across a league. During play, each match streams events as they happen — goals, cards, substitutions — into an event ingestion mechanism. Your service consumes that stream and must support two very different readers:

1. A **live scoreboard** that needs each match's **current state** (score, scorers, cards) queryable with **low latency**.
2. A separate **highlights / stats consumer** that needs to **replay a full match's event history** independently, later.

Some realities to design for:
- Bursts are **spiky** — a goal sets off a flurry of events, and a Saturday 3pm kickoff means many matches start at once.
- **Order within a match matters** (a goal then its disallowal); order *across* matches doesn't.
- Clients are imperfect: some events are **duplicated** (retries) and some are **malformed**.

## Your two decisions

Name your choice and, in a short paragraph each, justify it against the scenario's forces and the alternatives you rejected:

1. **Ingestion mechanism** — how events get from clients into your service.
2. **Current-state store** — where the live scoreboard reads from.

There's no single right answer; we care about the reasoning and the trade-offs you weighed. Capture each decision in a short **architecture decision record** (a paragraph or two each — chosen option, alternatives rejected, and when the alternative would have won). You can find more information on ADRs at https://learn.microsoft.com/en-us/azure/well-architected/architect-role/architecture-decision-record.

## What you build (the vertical slice)

A thin but real slice that proves your choices work:

- A **consumer** that ingests match events and maintains **current state per match**.
- **Idempotent** updates: a duplicate or replayed `GOAL` event must **not** turn a 2-0 into a 3-0. Replaying the stream must leave the scores correct.
- **Malformed events** (e.g. a goal with no scorer, unparseable payload) must not crash the scoreboard — route them aside with a reason.
- **Two read endpoints** (HTTP is fine):
  - current state for a given match (or all live matches),
  - ingestion health — counts of processed / duplicate / rejected.
- **Infrastructure as Code** (Bicep *or* Terraform) for the topology you chose. It needn't be deployed; it must pass `validate` / `plan` (or `bicep build` / `what-if`) and a linter. Your IaC *is* your architecture made concrete.
- A **CI pipeline** (Azure DevOps YAML or GitHub Actions) that builds and runs your tests. A couple of focused tests on the parts that matter — dedupe and malformed input — are worth more than broad shallow ones.
- A short **README**: how to run it, the choices you made, and what you'd do with more time.

## Things we've left open on purpose

We haven't given you the exact event rate, what a duplicate should do beyond "not double-count," or how rich the match state needs to be. Make a reasonable call, **note your assumptions in the README**, and be ready to talk about them. Asking us a clarifying question first is also fine.

## Stack & setup

Everything should run locally — no Azure subscription needed. **Standing up your own local infra is part of the exercise**, so set up whatever fits the decisions you made. As a starting point, depending on what you choose:

- Event Hubs emulator — https://learn.microsoft.com/azure/event-hubs/overview-emulator
- Service Bus emulator — https://learn.microsoft.com/azure/service-bus-messaging/overview-emulator
- Azurite (Blob/Queue/Table) — https://learn.microsoft.com/azure/storage/common/storage-use-azurite
- Cosmos DB emulator — https://learn.microsoft.com/azure/cosmos-db/emulator

Not every Azure service has a local emulator (Event Grid, for instance) — if the option you chose doesn't, stub or simulate it locally and say so in your ADR.

**Event data:** we provide `event_generator.cs`, which emits the hostile stream (valid + duplicate + malformed events) as NDJSON to console or a file. It knows nothing about transport on purpose — **getting that data into the ingestion mechanism you chose is part of the exercise.** You're free to write your own generator instead; this one just saves you building a realistic messy dataset.

**Consumer/API:** any stack you like. We're a .NET shop, so .NET is a natural fit, but use what lets you move fastest.

## Friday

You'll present for about 30 minutes: a short walkthrough, then we dig into *why* you chose what you chose, when the alternatives would have won, and a small live change together (AI allowed). Come ready to defend trade-offs, not just demo the happy path.

Have fun with it.
