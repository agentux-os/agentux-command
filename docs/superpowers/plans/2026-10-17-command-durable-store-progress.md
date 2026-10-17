# SDD ledger — plan: docs/superpowers/plans/2026-10-17-command-durable-store.md

Preflight: user confirmed continuation from the published Office vertical slice into Command issue #12. Worktree is based on origin/main at d864c84; original local checkout remains untouched.

Shared interfaces:
- Task 1 defines the persistence interface and schema consumed by Tasks 2–4.
- Task 2 event persistence and replay are consumed by Task 3 approval decisions and Task 4 runtime wiring.
- Task 3 adds `organizationId` to the approval request, shared with Apps and AUXP.

Global ruling: retain in-memory `NewHandler()` for unit/demo use while production startup uses PostgreSQL. This preserves the contract and local demo behavior while enforcing persistence in the deployed service; cost if wrong: local and production startup differ, so the endpoint contract and integration tests must cover both adapters.

Task 1: complete (commit 51402ab, tests: `go test ./...` → all packages pass; PostgreSQL runtime integration is deferred to Tasks 2 and 4).

Task 2: complete (commit 50bb2fd, tests: `go test ./... -count=1` and `go test -race ./internal/office ./internal/server -count=1` pass. Five live PostgreSQL integration cases compile but skip locally because `AGENTUX_COMMAND_TEST_DATABASE_URL` is unavailable; Task 4 adds a PostgreSQL-backed CI service to execute them.)

Task 3: complete (Command repository contract, Apps adapter, and AUXP response/request schema now carry organizationId; tests: Command `go test ./... -count=1`, Apps `npm test` → 4/4, Protocol `node scripts/validate-fixtures.mjs` → 3 valid and 2 invalid fixtures pass. Commits: command changes already in 50bb2fd; Apps 4c9ec02; Protocol 1b91eaa.)
