import test from "ava";
import { randomUUID } from "node:crypto";

import type { AsyncOperation } from "./bench-shared.js";
import {
  aggregateType,
  createBench,
  datasetSizes,
  eventsLimit,
  formatDatasetLabel,
  listLimit,
  logDatasetPreparation,
  projectionFields,
  runOperation,
  summarizeBench,
  toErrorMessage,
  validateBenchTasks,
} from "./bench-shared.js";

type PgConfig = {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  sslmode?: string;
};

const parsePgLibpqDsn = (dsn: string): PgConfig => {
  const result: PgConfig = {};
  const regex = /(\w+)=('([^']*)'|[^\s]+)/g;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(dsn)) !== null) {
    const [, key, rawValue, quotedValue] = match;
    const value = quotedValue ?? rawValue.replace(/^'(.*)'$/, "$1");
    if (!value) {
      continue;
    }
    switch (key) {
      case "host":
        result.host = value;
        break;
      case "port": {
        const parsed = Number.parseInt(value, 10);
        if (Number.isFinite(parsed)) {
          result.port = parsed;
        }
        break;
      }
      case "user":
        result.user = value;
        break;
      case "password":
        result.password = value;
        break;
      case "dbname":
      case "database":
        result.database = value;
        break;
      case "sslmode":
        result.sslmode = value;
        break;
      default:
        break;
    }
  }

  return result;
};

const buildPgPoolConfig = (dsn: string) => {
  if (dsn.includes("://")) {
    return { connectionString: dsn };
  }
  const parsed = parsePgLibpqDsn(dsn);
  const config: Record<string, unknown> = {
    host: parsed.host,
    port: parsed.port,
    user: parsed.user,
    password: parsed.password,
    database: parsed.database,
  };

  if (parsed.sslmode && parsed.sslmode.toLowerCase() === "require") {
    config.ssl = { rejectUnauthorized: false };
  }

  return config;
};

const ensurePostgresDataset = async (
  pool: {
    query: (
      text: string,
      params?: unknown[]
    ) => Promise<{ rows: Array<Record<string, unknown>> }>;
  },
  targetSize: number
) => {
  await pool.query("DELETE FROM bench_events WHERE category = $1", [
    aggregateType,
  ]);
  await pool.query("DELETE FROM bench_aggregates WHERE category = $1", [
    aggregateType,
  ]);

  const { rows } = await pool.query(
    "SELECT COUNT(*)::int AS count FROM bench_aggregates WHERE category = $1",
    [aggregateType]
  );
  const existing = Number(rows[0]?.count ?? 0);

  if (existing >= targetSize) {
    return existing;
  }

  await pool.query(
    `
      WITH new_items AS (
        SELECT generate_series($2::integer, $3::integer) AS idx
      )
      INSERT INTO bench_aggregates (aggregate_id, category, state, archived, updated_at)
      SELECT
        LPAD(idx::text, 16, '0'),
        $1,
        jsonb_build_object(
          'field1', 'value-' || LPAD(idx::text, 16, '0'),
          'field2', idx,
          'name', 'Account ' || LPAD(idx::text, 16, '0'),
          'archived', false
        ),
        false,
        NOW()
      FROM new_items
      ON CONFLICT (aggregate_id) DO NOTHING
    `,
    [aggregateType, existing + 1, targetSize]
  );

  await pool.query(
    `
      INSERT INTO bench_events (aggregate_id, category, event_type, payload, created_at)
      SELECT
        LPAD(g::text, 16, '0'),
        $1,
        'Created',
        jsonb_build_object(
          'field1', 'value-' || LPAD(g::text, 16, '0'),
          'field2', g,
          'name', 'Account ' || LPAD(g::text, 16, '0'),
          'version', 1
        ),
        NOW()
      FROM generate_series($2::integer, $3::integer) AS g
    `,
    [aggregateType, existing + 1, targetSize]
  );

  return targetSize;
};

test("benchmarks postgres operations", async (t) => {
  const dsn =
    process.env.EVENTDBX_PG_DSN ??
    process.env.PG_CONNECTION_STRING ??
    process.env.DATABASE_URL ??
    process.env.PGURL ??
    process.env.POSTGRES_URL;

  if (!dsn) {
    t.log(
      "Skipping Postgres benchmark – EVENTDBX_PG_DSN (or equivalent) not provided"
    );
    t.pass();
    return;
  }

  let pgModule: any;
  try {
    pgModule = await import("pg");
  } catch (error) {
    t.log(
      `Skipping Postgres benchmark – unable to load pg module: ${toErrorMessage(
        error
      )}`
    );
    t.pass();
    return;
  }

  const pool = new pgModule.Pool(buildPgPoolConfig(dsn));

  let connectionReady = true;
  try {
    await pool.query("SELECT 1");
  } catch (error) {
    connectionReady = false;
    t.log(
      `Skipping Postgres benchmark – unable to connect: ${toErrorMessage(
        error
      )}`
    );
  }

  if (!connectionReady) {
    await pool.end().catch(() => {});
    t.pass();
    return;
  }

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bench_events (
        id BIGSERIAL PRIMARY KEY,
        aggregate_id TEXT NOT NULL,
        category TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(
      "UPDATE bench_events SET aggregate_id = COALESCE(aggregate_id, id::text) WHERE aggregate_id IS NULL OR aggregate_id = ''"
    );
    await pool.query(
      "UPDATE bench_events SET event_type = COALESCE(event_type, 'Legacy') WHERE event_type IS NULL OR event_type = ''"
    );
    await pool.query(
      "CREATE INDEX IF NOT EXISTS bench_events_category_idx ON bench_events (category)"
    );
    await pool.query(
      "CREATE INDEX IF NOT EXISTS bench_events_aggregate_idx ON bench_events (aggregate_id)"
    );
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bench_aggregates (
        aggregate_id TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        state JSONB NOT NULL,
        archived BOOLEAN NOT NULL DEFAULT FALSE,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(
      "CREATE INDEX IF NOT EXISTS bench_aggregates_category_idx ON bench_aggregates (category)"
    );

    for (const [datasetIndex, size] of datasetSizes.entries()) {
      await ensurePostgresDataset(pool, size);
      logDatasetPreparation(t, "Postgres", size, datasetIndex);
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(
        size
      )})`;
      const pageSize = Math.max(1, Math.min(listLimit, Number(size)));

      const bench = createBench(`Postgres ${datasetLabel}`);

      const pickAggregateId = async () => {
        const { rows } = await pool.query(
          "SELECT aggregate_id FROM bench_aggregates WHERE category = $1 ORDER BY aggregate_id ASC LIMIT 1",
          [aggregateType]
        );
        const aggregateId = rows[0]?.aggregate_id as string | undefined;
        if (!aggregateId) {
          throw new Error("Postgres dataset is empty");
        }
        return aggregateId;
      };

      const operations: Array<[string, AsyncOperation]> = [
        [
          "list",
          () =>
            pool.query(
              "SELECT aggregate_id FROM bench_aggregates WHERE category = $1 AND archived = FALSE ORDER BY aggregate_id ASC LIMIT $2::int",
              [aggregateType, pageSize]
            ),
        ],
        [
          "get",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "SELECT state FROM bench_aggregates WHERE aggregate_id = $1",
              [aggregateId]
            );
          },
        ],
        [
          "select",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              `SELECT ${projectionFields
                .map(
                  (field) =>
                    `state ->> '${field.split(".").at(-1)}' AS "${field}"`
                )
                .join(", ")} FROM bench_aggregates WHERE aggregate_id = $1`,
              [aggregateId]
            );
          },
        ],
        [
          "events",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "SELECT event_type, payload FROM bench_events WHERE aggregate_id = $1 ORDER BY created_at ASC LIMIT $2::int",
              [aggregateId, eventsLimit]
            );
          },
        ],
        [
          "apply",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "INSERT INTO bench_events (aggregate_id, category, event_type, payload, created_at) VALUES ($1, $2, $3, $4, NOW())",
              [
                aggregateId,
                aggregateType,
                "BenchApplied",
                { marker: "apply", at: new Date().toISOString() },
              ]
            );
          },
        ],
        [
          "create",
          async () => {
            const aggregateId = `bench-${randomUUID()}`;
            await pool.query(
              "INSERT INTO bench_aggregates (aggregate_id, category, state, archived, updated_at) VALUES ($1, $2, $3, FALSE, NOW())",
              [
                aggregateId,
                aggregateType,
                {
                  field1: "value-bench",
                  field2: 0,
                  name: "Benchmark Account",
                  archived: false,
                },
              ]
            );
            await pool.query(
              "INSERT INTO bench_events (aggregate_id, category, event_type, payload, created_at) VALUES ($1, $2, $3, $4, NOW())",
              [
                aggregateId,
                aggregateType,
                "Created",
                {
                  name: "Benchmark Account",
                  createdAt: new Date().toISOString(),
                  field1: "value-bench",
                  field2: 0,
                },
              ]
            );
          },
        ],
        [
          "archive",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "UPDATE bench_aggregates SET archived = TRUE, state = jsonb_set(state, '{archived}', 'true'::jsonb), updated_at = NOW() WHERE aggregate_id = $1",
              [aggregateId]
            );
          },
        ],
        [
          "restore",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "UPDATE bench_aggregates SET archived = FALSE, state = jsonb_set(state, '{archived}', 'false'::jsonb), updated_at = NOW() WHERE aggregate_id = $1",
              [aggregateId]
            );
          },
        ],
        [
          "patch",
          async () => {
            const aggregateId = await pickAggregateId();
            await pool.query(
              "UPDATE bench_aggregates SET state = jsonb_set(state, '{name}', to_jsonb('New Name'::text)), updated_at = NOW() WHERE aggregate_id = $1",
              [aggregateId]
            );
            await pool.query(
              "INSERT INTO bench_events (aggregate_id, category, event_type, payload, created_at) VALUES ($1, $2, $3, $4, NOW())",
              [
                aggregateId,
                aggregateType,
                "Patched",
                { name: "New Name", at: new Date().toISOString() },
              ]
            );
          },
        ],
      ];

      for (const [label, action] of operations) {
        bench.add(label, async () => runOperation(label, action));
      }

      await bench.run();

      validateBenchTasks(t, bench);
      t.log(summarizeBench(bench, `Postgres ${datasetLabel}`));
      t.log("");
    }
  } finally {
    await pool.end().catch(() => {});
  }
});
