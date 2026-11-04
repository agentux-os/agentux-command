# Command benchmark harness design

**Intent:** Provide repeatable workload measurements for the single-host, single-Command-process, PostgreSQL topology in `docs/production-readiness.md` and issue #52.

**Goals:** Exercise event ingestion, heartbeats, concurrent mission transitions, Office replay/projection/live streams, and retention/database growth; record throughput, p50/p95/p99 latency, errors, target-process CPU/memory, database size, host/software metadata, and bounded overload behavior. Use a standard-library Python runner and operator-supplied scenario/configuration so no benchmark-only service code or new runtime dependency is needed.

**Non-goals:** Claim universal capacity numbers; run destructive retention against production; qualify multi-process, HA, or multi-host deployments; or invent results absent a controlled hardware run.

**Constraints:** Match the #49 profile topology; bound worker count and request timeout; record failures and rejected/overload outcomes; report missing resource/database instrumentation clearly; preserve raw machine-readable output; label repository defaults and unexecuted measurements `UNQUALIFIED`.

**Approaches:** Direct database microbenchmarks would miss API and live-stream costs. A dedicated Rust benchmark binary would couple load tooling to the service and require additional dependencies. The chosen configurable Python HTTP runner uses existing endpoints, bounded standard-library concurrency, and optional PostgreSQL/process observations; scenario payloads and preconditions stay explicit in a checked-in example.

**Builder calls:** Keep retention destructive action operator-triggered and outside the runner; compare database sizes before/after separately. Use scripted repetitions and report observed percentiles, without statistical claims beyond the sample.
