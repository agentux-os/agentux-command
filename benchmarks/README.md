# AgentUX Command capacity benchmarks

This harness measures a running Command HTTP API and its PostgreSQL database. It does not manufacture capacity figures: no target-hardware run has been captured in this repository, so every production limit in the [support profile](../docs/production-readiness.md#capacity-and-retention-envelope) remains **UNQUALIFIED**.

## Supported test topology

Use the #49 profile topology: one host, one Command process, and one PostgreSQL instance. Record the Command image digest, OS/kernel, CPU model/core count, RAM, disk/filesystem and free space, PostgreSQL version/configuration, Python version, benchmark commit, and topology. Use an isolated staging database with backups and a dedicated organization. Never run event ingestion, mission mutations, or pruning against production just to benchmark. The harness only sends requests; it never deletes data.

Python 3.10+ is sufficient. Optional process CPU/RSS sampling requires `pip install psutil`; database-size sampling requires `psql` and a least-privilege connection URL. These optional tools do not affect request traffic. Missing collectors are reported as unavailable, never as zero.

## Prepare identities and data

Copy `scenarios.example.json` to a private run directory and adjust `base_url`, `command_image_digest`, organization, node, request count, concurrency, timeout, and scenario mix. Set `BENCH_NODE_TOKEN` to the dedicated node bearer token and `BENCH_OPERATOR_TOKEN` to a token with organization read and node command capabilities. Environment variables keep credentials out of the JSON report; never put token values in the scenario file.

Before running the full file, seed the isolated organization with:

- one approved node per worker lane, named `bench-node-1` through `bench-node-concurrency`, with `BENCH_NODE_TOKEN` configured for every node and no prior cursor required (each event lane starts at sequence 1);
- enough existing Office events to make replay representative of the intended retained log size;
- a current projection for `bench-org`;
- one distinct active, pausable mission run per request, named `bench-active-run-1` through `bench-active-run-N`, using a definition that can be paused;
- an event stream cursor that can deliver at least one event (the runner records time to first byte and closes that stream).

The example uses a deterministic occurred-at timestamp for reproducibility; use a current timestamp if the deployment enforces freshness. Each fixed worker uses its own node ID and sequential cursor while `{index}` keeps event IDs and mission IDs unique across lanes. For another invocation, use a fresh event/run prefix or disposable organization and a fresh set of active mission runs. The example order ingests events and heartbeats before replay/projection/stream reads and mission transitions.

## Run and retain evidence

Run from the repository root so the report captures the benchmark commit and host disk context:

```sh
python benchmarks/command_bench.py --config /secure/run/scenarios.json \
  --pid COMMAND_PID --database-url "$BENCH_DATABASE_URL" \
  --output /secure/run/report.json
```

`--pid` samples the Command process at 500 ms intervals and reports average/maximum CPU and peak RSS for the whole run. `--database-url` records `pg_database_size`, PostgreSQL version and `shared_buffers` before and after and the net size difference; it is a coarse database-wide delta, not per-table growth or WAL/archive growth. The URL is not included in output or passed on the psql command line. Keep raw JSON, scenario config (with secrets removed), exact command, stdout/stderr, Command logs, other PostgreSQL settings, and a SHA-256 of the report together as the evidence record. A nonzero exit means at least one scenario failed; the report still includes the failures.

Run each scenario family independently as well as the mixed workload so its cost is attributable. At minimum cover:

1. **Event ingestion/replay:** vary existing log depth, event payload size, organization count, and event rate. Record unique IDs and verify projection correctness after each run.
2. **Heartbeats:** vary node count, heartbeat cadence, reconnect bursts, and concurrency; use approved nodes and valid bearer credentials.
3. **Mission transitions:** use unique pre-seeded active runs and vary concurrent pause/resume/cancel transitions and operator reads. Do not reuse a run after its transition is complete.
4. **Office reads/live streams:** vary replay history/cursor and concurrent first-byte live-stream clients; record read request and stream first-byte latency separately.
5. **Database growth/retention:** run a one-scenario config around a fixed event batch to attribute the reported before/after database delta. For retention, record database/archive size, take a backup, then run the existing CLI in disposable staging:

   ```sh
   podman exec agentux-command agentux-command prune-events --older-than-days 365 --archive /var/lib/agentux-command/archive --dry-run
   podman exec agentux-command agentux-command prune-events --older-than-days 365 --archive /var/lib/agentux-command/archive
   podman exec agentux-command agentux-command check
   ```

   Record size after the dry run and separately approved real prune, and verify projection/replay correctness. Retention requires an archive destination and backup; compare database and archive bytes. The harness does not invoke pruning.

Repeat at increasing `concurrency` values (for example 1, 2, 4, 8, 16, 32, 64) with a fixed workload and then with increasing workload size. Keep the single Command process and host/database configuration fixed. For overload, raise offered concurrency until latency, errors, or resource pressure changes; record 429/5xx responses, timeouts, refused connections, process/database availability, and whether memory/connection/queue use stays bounded. The runner caps each scenario at 100,000 requests and 512 client workers, with request timeouts no longer than 300 seconds. Do not describe a client-side worker cap as a server-side resource bound.

## Metrics and qualification

Each scenario reports attempted/successful/error requests, status counts, elapsed time, throughput, and p50/p95/p99 request latency in milliseconds; raw latencies and up to 20 error samples are retained. Process samples report CPU average/max and peak RSS. Database sampling reports before/after total database bytes and net growth for the entire invocation (so use a one-scenario config for attribution); it does not separately measure table, WAL, or archive growth. Hardware and workload settings travel in the JSON report.

Use multiple clean repetitions at each workload point. A limit is qualified only for the exact tested topology, build, hardware, PostgreSQL settings, workload mix/size, concurrency, and run duration. Define the acceptable latency/error/resource criteria before running; the #49 profile does not set capacity SLO thresholds. Publish the highest repeatable workload meeting those criteria and the first overload point with raw evidence. A result on other hardware, in-memory tests, or an unrepresentative synthetic scenario does not qualify the profile. Update `docs/production-readiness.md` only when evidence is retained and linked.

## Scenario format

Each scenario has a unique `name`, HTTP `method`, absolute-path `path`, optional `json` request body, optional `token_env`, and optional `read_first_byte` for event streams. String values replace `{index}` with a one-based global request number, `{lane}` with a one-based worker/node lane, and `{lane_sequence}` with a per-lane one-based integer sequence number. The runner uses a fixed worker pool and keeps each lane sequential, so task scheduling memory does not grow with total request count and event cursors do not race one another. Non-2xx/3xx responses and transport errors count as errors; overload responses are therefore visible rather than counted as successes.
