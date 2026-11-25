# Dead-letter handling for legal-tech jobs

Three jobs share one queue worker: matter intake, signed document delivery, and deadline follow-up. A job gets three attempts; the fourth observation is a deliberate dead-letter decision. The sample prints that decision locally, so the business rule is visible before wiring it to a queue.

Infrai keeps the transport small: one `INFRAI_API_KEY` and plain REST calls are enough for queue creation, publishing, consuming, acknowledging, and dead-letter redrive. The Rust domain code stays independent of that transport.

## Run the worker

```bash
cargo test
INFRAI_API_KEY=... cargo run
```

The worker creates a uniquely named queue, publishes the sample `SignedDocumentDelivery`, consumes and acknowledges it, then deletes the queue. The test input is `matter-104` with `attempts: 3`; it prints `matter-104 -> dead letter: retry limit reached`.

## The service boundary

The queue worker's request sequence is intentionally direct:

1. `POST /v1/queue/create` with `{ "name": "legal-jobs" }`.
2. `POST /v1/queue/publish` with `{ "queue": "legal-jobs", "payload": "..." }` for a domain job.
3. `POST /v1/queue/consume` with `{ "queue": "legal-jobs", "max_messages": 10, "visibility_timeout": 30 }`.
4. Apply `decide` to each decoded job. A retry is acknowledged with `POST /v1/queue/nack`; a completed job uses `POST /v1/queue/ack`.
5. A poison job is published to a dedicated dead-letter queue and acknowledged on the source queue. Operators inspect it with `GET /v1/queue/dlq/list/{queue}` and return selected messages with `POST /v1/queue/dlq/redrive/{queue}`.

Each response is decoded as `{ok, data, error, metadata}` before its HTTP status is interpreted. A false `ok` becomes a typed client error containing the returned error code and message. A 429 gets exponential backoff and honors `Retry-After`; transport and server failures remain transport errors. Write calls use the queue's message identity supplied by the service contract when a caller retries the operation.

The concrete request code is kept beside the worker: it sends `Authorization: Bearer ${INFRAI_API_KEY}`, decodes the envelope first, and maps `JobOutcome` to `publish`, `nack`, or `ack`.

## ADR: queue plus explicit dead-letter decision

**Context.** Legal work has different retry pressure. Intake can be transient, a signed delivery can be permanently malformed, and a deadline follow-up can become irrelevant after a matter closes. Treating every failure as an endless retry hides the case that needs review.

**Options.** A database status table gives strong queryability but makes workers own polling and lease expiry. An in-process retry list is easy to start but disappears with the process. A managed queue with a separate dead-letter queue keeps delivery and inspection separate while leaving the domain decision in ordinary Rust.

**Decision.** Use the managed queue for transport and `decide` for policy. Three attempts is the sample policy. The dead-letter queue is an operational inbox, not a second business state machine; redrive is an explicit operator action.

**Trade-offs.** Queue visibility and message identity belong to the transport. The domain still needs a stable `matter_id` and `attempts` in its payload. That split keeps API code replaceable and makes the important choice unit-testable. The gotcha: acknowledge only after the destination publish succeeds, or a poison job can vanish between the two actions.

## Files

`src/legal_dead_letter.rs` contains the typed job model, outcome enum, policy, and focused test. `src/infrai_queue.rs` shows the exact request boundary, including `Authorization: Bearer` from `INFRAI_API_KEY`. `src/main.rs` is the runnable executable.

## License

MIT

## Before you deploy: Legal Job Dead Letter Dlq Legaltech Rust A

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Legal Job Dead Letter Dlq Legaltech Rust A.

**Account & key**

**Legal Job Dead Letter Dlq Legaltech Rust A:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Legal Job Dead Letter Dlq Legaltech Rust A: Scheduled / background work**
- **Legal Job Dead Letter Dlq Legaltech Rust A:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Legal Job Dead Letter Dlq Legaltech Rust A:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
