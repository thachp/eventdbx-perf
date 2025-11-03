import test from "ava";
import { randomUUID } from "node:crypto";

import type { AsyncOperation } from "./bench-shared.js";
import {
  addBenchTask,
  aggregateType,
  benchRunMode,
  createBench,
  datasetSizes,
  eventsLimit,
  filterBenchOperations,
  formatDatasetLabel,
  listLimit,
  loadOptionalModule,
  logDatasetPreparation,
  runOperation,
  summarizeBench,
  toErrorMessage,
  validateBenchTasks,
} from "./bench-shared.js";

type MssqlQueryResult = {
  recordset: Array<Record<string, unknown>>;
};

type MssqlRequest = {
  input: (name: string, type: unknown, value: unknown) => MssqlRequest;
  query: (query: string) => Promise<MssqlQueryResult>;
};

type MssqlConnectionPool = {
  request: () => MssqlRequest;
  close: () => Promise<void>;
};

type MssqlModule = {
  ConnectionPool: new (connectionString: string) => {
    connect: () => Promise<MssqlConnectionPool>;
  };
  NVarChar: unknown;
  Int: unknown;
};

type EnsureContext = {
  pool: MssqlConnectionPool;
  sql: Pick<MssqlModule, "NVarChar" | "Int">;
};

const isMssqlTimeoutError = (error: unknown) => {
  if (!error || typeof error !== "object") {
    return false;
  }

  const candidate = error as {
    code?: unknown;
    originalError?: { code?: unknown };
    message?: unknown;
  };

  const normalizeCode = (value: unknown) =>
    typeof value === "string" ? value.toUpperCase() : undefined;

  const candidates = [
    normalizeCode(candidate.code),
    normalizeCode(candidate.originalError?.code),
  ];

  if (candidates.includes("ETIMEOUT")) {
    return true;
  }

  if (typeof candidate.message === "string") {
    const message = candidate.message.toUpperCase();
    if (message.includes("ETIMEOUT") || message.includes("TIMED OUT")) {
      return true;
    }
  }

  return false;
};

const ensureMssqlDataset = async (
  { pool, sql }: EnsureContext,
  targetSize: number
) => {
  const request = pool.request();

  await request.query(`
    IF OBJECT_ID('dbo.benchEvents', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.benchEvents (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        aggregate_id NVARCHAR(255) NOT NULL,
        category NVARCHAR(100) NOT NULL,
        event_type NVARCHAR(100) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      );
      CREATE INDEX IX_benchEvents_category ON dbo.benchEvents(category);
      CREATE INDEX IX_benchEvents_aggregate ON dbo.benchEvents(aggregate_id);
    END;

    IF OBJECT_ID('dbo.benchAggregates', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.benchAggregates (
        aggregate_id NVARCHAR(255) NOT NULL PRIMARY KEY,
        category NVARCHAR(100) NOT NULL,
        state NVARCHAR(MAX) NOT NULL,
        archived BIT NOT NULL DEFAULT 0,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      );
      CREATE INDEX IX_benchAggregates_category ON dbo.benchAggregates(category);
    END;
  `);

  await pool.request().input("category", sql.NVarChar, aggregateType).query(`
    DELETE FROM dbo.benchEvents WHERE category = @category AND payload LIKE '%"benchDataset":true%'
  `);

  await pool.request().input("category", sql.NVarChar, aggregateType).query(`
    DELETE FROM dbo.benchAggregates WHERE category = @category AND state LIKE '%"benchDataset":true%'
  `);

  const { recordset } = await pool
    .request()
    .input("category", sql.NVarChar, aggregateType)
    .query(
      `SELECT COUNT(1) AS count FROM dbo.benchAggregates WHERE category = @category`
    );

  const existing = Number(recordset[0]?.count ?? 0);
  if (existing >= targetSize) {
    return existing;
  }

  await pool
    .request()
    .input("category", sql.NVarChar, aggregateType)
    .input("from", sql.Int, existing + 1)
    .input("to", sql.Int, targetSize).query(`
      WITH numbers AS (
        SELECT TOP (@to - @from + 1)
          ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + @from - 1 AS idx
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
      )
      INSERT INTO dbo.benchAggregates (aggregate_id, category, state, archived, updated_at)
      SELECT
        agg.aggregate_id,
        @category,
        (
          SELECT
            CONCAT('value-', agg.aggregate_id) AS field1,
            idx AS field2,
            CONCAT('Account ', agg.aggregate_id) AS name,
            CAST(0 AS BIT) AS archived,
            CAST(1 AS BIT) AS benchDataset
          FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        0,
        SYSUTCDATETIME()
      FROM numbers
      CROSS APPLY (
        SELECT RIGHT(CONCAT(REPLICATE('0', 16), CAST(numbers.idx AS NVARCHAR(16))), 16) AS aggregate_id
      ) AS agg
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.benchAggregates a
        WHERE a.aggregate_id = agg.aggregate_id
      );
    `);

  await pool
    .request()
    .input("category", sql.NVarChar, aggregateType)
    .input("from", sql.Int, existing + 1)
    .input("to", sql.Int, targetSize).query(`
      WITH numbers AS (
        SELECT TOP (@to - @from + 1)
          ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + @from - 1 AS idx
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
      )
      INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at)
      SELECT
        agg.aggregate_id,
        @category,
        'Created',
        (
          SELECT
            CONCAT('value-', agg.aggregate_id) AS field1,
            idx AS field2,
            CONCAT('Account ', agg.aggregate_id) AS name,
            CAST(1 AS INT) AS version,
            CAST(1 AS BIT) AS benchDataset
          FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        SYSUTCDATETIME()
      FROM numbers
      CROSS APPLY (
        SELECT RIGHT(CONCAT(REPLICATE('0', 16), CAST(numbers.idx AS NVARCHAR(16))), 16) AS aggregate_id
      ) AS agg;
    `);

  return targetSize;
};

test("benchmarks mssql operations", async (t) => {
  const connectionString =
    process.env.EVENTDBX_MSSQL_CONN ??
    process.env.MSSQL_CONNECTION_STRING ??
    process.env.MSSQL_URL ??
    process.env.SQLSERVER_URL ??
    "Server=localhost;Database=ERPDb;User Id=SA;Password=bench;TrustServerCertificate=True";

  if (!connectionString) {
    t.log(
      "Skipping MSSQL benchmark – EVENTDBX_MSSQL_CONN (or equivalent) not provided"
    );
    t.pass();
    return;
  }

  const { module: sqlModule, error: sqlLoadError } = await loadOptionalModule<
    typeof import("mssql")
  >("mssql");
  if (!sqlModule) {
    const message =
      sqlLoadError !== undefined
        ? `Skipping MSSQL benchmark – unable to load mssql module: ${toErrorMessage(
            sqlLoadError
          )}`
        : "Skipping MSSQL benchmark – mssql module not available";
    t.log(message);
    t.pass();
    return;
  }

  const sql = sqlModule as MssqlModule;

  let pool: MssqlConnectionPool;
  try {
    pool = await new sql.ConnectionPool(connectionString).connect();
  } catch (error) {
    t.log(
      `Skipping MSSQL benchmark – unable to connect: ${toErrorMessage(error)}`
    );
    t.pass();
    return;
  }

  try {
    const skipOnTimeout = (error: unknown) => {
      if (isMssqlTimeoutError(error)) {
        t.log(
          `Skipping MSSQL benchmark – request timed out: ${toErrorMessage(
            error
          )}`
        );
        t.pass();
        return true;
      }
      return false;
    };

    for (const [datasetIndex, size] of datasetSizes.entries()) {
      try {
        await ensureMssqlDataset({ pool, sql }, size);
      } catch (error) {
        if (skipOnTimeout(error)) {
          return;
        }
        throw error;
      }
      logDatasetPreparation(t, "MSSQL", size, datasetIndex);
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(
        size
      )})`;
      const pageSize = Math.max(1, Math.min(listLimit, Number(size)));

      const bench = createBench(`MSSQL ${datasetLabel}`);

      const { recordset: aggregateRecords } = await pool
        .request()
        .input("category", sql.NVarChar, aggregateType)
        .query(
          "SELECT aggregate_id FROM dbo.benchAggregates WHERE category = @category ORDER BY aggregate_id ASC"
        );

      const aggregateIds = aggregateRecords
        .map((row) => row.aggregate_id as string | undefined)
        .filter((value): value is string => Boolean(value));
      if (aggregateIds.length === 0) {
        throw new Error("MSSQL aggregate set is empty");
      }
      let aggregateCursor = 0;
      const pickAggregateId = () => {
        const aggregateId = aggregateIds[aggregateCursor];
        aggregateCursor = (aggregateCursor + 1) % aggregateIds.length;
        return aggregateId;
      };

      const operations: Array<[string, AsyncOperation]> = filterBenchOperations(
        [
          [
            "list",
            async () => {
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("limit", sql.Int, pageSize)
                .query(
                  "SELECT TOP (@limit) * FROM dbo.benchAggregates WHERE category = @category AND archived = 0 ORDER BY aggregate_id ASC"
                );
            },
          ],
          [
            "get",
            async () => {
              const aggregateId = pickAggregateId();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .query(
                  "SELECT state FROM dbo.benchAggregates WHERE category = @category AND aggregate_id = @id"
                );
            },
          ],
          [
            "select",
            async () => {
              const aggregateId = pickAggregateId();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .query(
                  `SELECT JSON_VALUE(state, '$.field1') AS field1, JSON_VALUE(state, '$.field2') AS field2 FROM dbo.benchAggregates WHERE category = @category AND aggregate_id = @id`
                );
            },
          ],
          [
            "events",
            async () => {
              const aggregateId = pickAggregateId();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input("limit", sql.Int, eventsLimit)
                .query(
                  "SELECT TOP (@limit) event_type, payload FROM dbo.benchEvents WHERE category = @category AND aggregate_id = @id ORDER BY created_at ASC"
                );
            },
          ],
          [
            "apply",
            async () => {
              const aggregateId = pickAggregateId();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    marker: "apply",
                    at: new Date().toISOString(),
                  })
                )
                .query(
                  "INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at) VALUES (@id, @category, 'BenchApplied', @payload, SYSUTCDATETIME())"
                );
            },
          ],
          [
            "create",
            async () => {
              const aggregateId = `bench-${randomUUID()}`;
              const payload = JSON.stringify({
                field1: "value-bench",
                field2: 0,
                name: "Benchmark Account",
                createdAt: new Date().toISOString(),
                archived: false,
              });

              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input("state", sql.NVarChar, payload)
                .query(
                  "INSERT INTO dbo.benchAggregates (aggregate_id, category, state, archived, updated_at) VALUES (@id, @category, @state, 0, SYSUTCDATETIME())"
                );

              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    name: "Benchmark Account",
                    createdAt: new Date().toISOString(),
                  })
                )
                .query(
                  "INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at) VALUES (@id, @category, 'Created', @payload, SYSUTCDATETIME())"
                );
            },
          ],
          [
            "archive",
            async () => {
              const aggregateId = pickAggregateId();
              const archivedAt = new Date().toISOString();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .query(
                  "UPDATE dbo.benchAggregates SET archived = 1, state = JSON_MODIFY(state, '$.archived', 1), updated_at = SYSUTCDATETIME() WHERE category = @category AND aggregate_id = @id"
                );
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    note: "benchmark archive",
                    at: archivedAt,
                  })
                )
                .query(
                  "INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at) VALUES (@id, @category, 'Archived', @payload, SYSUTCDATETIME())"
                );
            },
          ],
          [
            "restore",
            async () => {
              const aggregateId = pickAggregateId();
              const restoredAt = new Date().toISOString();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .query(
                  "UPDATE dbo.benchAggregates SET archived = 0, state = JSON_MODIFY(state, '$.archived', 0), updated_at = SYSUTCDATETIME() WHERE category = @category AND aggregate_id = @id"
                );
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    note: "benchmark restore",
                    at: restoredAt,
                  })
                )
                .query(
                  "INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at) VALUES (@id, @category, 'Restored', @payload, SYSUTCDATETIME())"
                );
            },
          ],
          [
            "patch",
            async () => {
              const aggregateId = pickAggregateId();
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    name: "New Name",
                    at: new Date().toISOString(),
                  })
                )
                .query(
                  "UPDATE dbo.benchAggregates SET state = JSON_MODIFY(state, '$.name', 'New Name'), updated_at = SYSUTCDATETIME() WHERE category = @category AND aggregate_id = @id"
                );
              await pool
                .request()
                .input("category", sql.NVarChar, aggregateType)
                .input("id", sql.NVarChar, aggregateId)
                .input(
                  "payload",
                  sql.NVarChar,
                  JSON.stringify({
                    name: "New Name",
                    at: new Date().toISOString(),
                  })
                )
                .query(
                  "INSERT INTO dbo.benchEvents (aggregate_id, category, event_type, payload, created_at) VALUES (@id, @category, 'Patched', @payload, SYSUTCDATETIME())"
                );
            },
          ],
        ],
        {
          onSkip: (label) =>
            t.log(
              `Skipping ${label} operation in mode "${benchRunMode}" for MSSQL benchmark`
            ),
        }
      );

      if (operations.length === 0) {
        t.log(
          `No operations enabled for MSSQL dataset ${datasetLabel} with run mode "${benchRunMode}". Skipping.`
        );
        continue;
      }

      for (const [label, action] of operations) {
        addBenchTask(bench, label, () => runOperation(label, action));
      }

      try {
        await bench.run();
      } catch (error) {
        if (skipOnTimeout(error)) {
          return;
        }
        throw error;
      }

      validateBenchTasks(t, bench);
      t.log(summarizeBench(bench, `MSSQL ${datasetLabel}`));
      t.log("");
    }
  } finally {
    await pool.close().catch(() => {});
  }
});
