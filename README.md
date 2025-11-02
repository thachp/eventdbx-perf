# EventDBX Performance Benchmarks

This repository contains the TypeScript benchmark harnesses we use to exercise
EventDBX alongside the databases it most often competes or interoperates with
(PostgreSQL, MongoDB, Microsoft SQL Server, and the EventDBX control API).
Every suite is designed to be an apples-to-apples comparison:

- All containers run locally on the same Docker bridge network.
- Each service is started with identical CPU and memory limits (see
  `src/docker-compose.yml`).
- Dataset sizes are identical across backends at each benchmark tier.
- The Tinybench harness, operation labels, and success validation are shared
  verbatim between implementations.

The specs themselves seed synthetic data, execute a consistent set of
CRUD-style operations with [Tinybench](https://github.com/tinylibs/tinybench),
and print summarised throughput/latency statistics.

All benchmark sources live in `src/__tests__`. They must be transpiled to
`dist/__tests__` before AVA consumes them; the provided pnpm scripts wrap this
flow so you rarely have to invoke `tsc` directly.

## Prerequisites

- Node.js 18.18+ (the project uses ES modules and top-level `await`).
- Local or remote database instances that match the connection details in your
  environment (see below). A `docker-compose` file is available under
  `src/docker-compose.yml` for quick local provisioning.

## Installing & running

```bash
pnpm install         # install dependencies
pnpm test            # build the TypeScript sources and run all benchmarks
```

Running `pnpm test` performs a TypeScript build (`pnpm run build` →
`tsc --project tsconfig.json`)
and then invokes AVA against `dist/__tests__/**/*.spec.js`. Each suite attempts
to connect to its backend and will log a skip message (rather than fail) if the
service is unreachable or the optional driver is not installed.

## Test layout

| Spec                     | Backend              | Highlights                                                                       |
| ------------------------ | -------------------- | -------------------------------------------------------------------------------- |
| `bench-eventdbx.spec.ts` | EventDBX control API | Exercises the `eventdbxjs` client with a fixed auth token.                       |
| `bench-postgres.spec.ts` | PostgreSQL           | Seeds zero-padded string aggregate IDs so ordering works without casts.          |
| `bench-mongodb.spec.ts`  | MongoDB              | Bulk inserts synthetic documents and benchmarks common event-store operations.   |
| `bench-mssql.spec.ts`    | Microsoft SQL Server | Detects `ETIMEOUT` responses and treats them as skips rather than hard failures. |
| `bench-shared.ts`        | Shared utilities     | Dataset definitions, optional module loader, formatting helpers, etc.            |

Default dataset sizes are defined in `bench-shared.ts` as
`[1_000, 10_000, 100_000, 1_000_000]`.

## Running a single suite

You can limit AVA to individual suites (or specific benchmark names) by calling
it directly or by forwarding arguments through the pnpm script:

```bash
# Build once, then run only the PostgreSQL benchmarks
pnpm run build
pnpm exec ava "dist/__tests__/bench-postgres.spec.js"

# Same suite, but constrain to operations whose labels contain "apply"
pnpm exec ava "dist/__tests__/bench-postgres.spec.js" --match="*apply*"
```

While iterating, you can also use AVA’s watch mode (`pnpm exec ava --watch ...`)
in combination with `pnpm exec tsc --watch` if you prefer continuous feedback.

## Environment configuration

`bench-shared.ts` loads environment variables once via `dotenv/config`. A sample
`.env` lives at the project root and already mirrors the defaults shown below;
feel free to copy/modify it for your environment.

| Variable                              | Default value                                                                 | Description                                  |
| ------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------- |
| `EVENTDBX_MSSQL_CONN`                 | `Server=localhost;User Id=SA;Password=<Password>;TrustServerCertificate=True` | Connection string for MSSQL.                 |
| `EVENTDBX_MONGO_URI`                  | `mongodb://localhost:27017`                                                   | MongoDB connection URI.                      |
| `EVENTDBX_MONGO_DB`                   | `bench`                                                                       | Mongo database name.                         |
| `EVENTDBX_MONGO_COLLECTION`           | `events`                                                                      | Events collection name.                      |
| `EVENTDBX_MONGO_AGGREGATE_COLLECTION` | `events_aggregates`                                                           | Aggregates collection name.                  |
| `EVENTDBX_PG_DSN`                     | `postgresql://bench:bench@localhost:5432/bench`                               | PostgreSQL DSN consumed by `pg.Pool`.        |
| `EVENTDBX_TEST_IP`                    | `127.0.0.1`                                                                   | EventDBX control client host.                |
| `EVENTDBX_TEST_PORT`                  | `6363`                                                                        | EventDBX control client port.                |
| `EVENTDBX_TEST_TOKEN`                 | _static JWT in repo_                                                          | Authentication token for the control client. |

To spin up local dependencies quickly, use the provided compose file (passwords
match the defaults above):

```bash
docker compose -f src/docker-compose.yml up -d postgres mongodb mssql
```

Remember to tear the stack down afterwards (`docker compose down`).

## Troubleshooting tips

- **Timeouts when talking to MSSQL**: the harness treats `ETIMEOUT` responses as
  skips and logs the failure. Double-check that port `1433` is reachable and the
  SA password matches `EVENTDBX_MSSQL_CONN` if you expect the benchmark to run.
- **`Cannot find module 'pg'` (or similar)**: optional drivers are loaded at
  runtime. Install the relevant package (`npm install pg`, `npm install mssql`,
  etc.) or unset the corresponding environment variables to skip that backend.
- **Postgres progress parser warnings about casts**: aggregate IDs are seeded as
  zero-padded strings and queries order lexicographically. If you still see
  warnings, ensure you are running the updated suite (no explicit casts remain).

## Cleaning up

The benchmark specs delete existing synthetic data before seeding, but long
running sessions can still leave behind large datasets. Clean up per-backend
data directories as needed to reclaim space.
