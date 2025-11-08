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
  formatAggregateId,
  listLimit,
  logDatasetPreparation,
  logRunModeNotice,
  projectionFields,
  runOperation,
  summarizeBench,
  toErrorMessage,
  validateBenchTasks,
} from "./bench-shared.js";

import test from "ava";

import { createClient } from "eventdbxjs";

type RuntimeEnv = Record<string, string | undefined>;
type RuntimeContext = { process?: { env?: RuntimeEnv } };

const runtimeEnv: RuntimeEnv =
  (globalThis as RuntimeContext).process?.env ?? {};

const baseOptions = {
  ip: "127.0.0.1",
  port: 6363,
  token: process.env.EVENTDBX_CONTROL_TOKEN ?? "test-token",
};

type ControlClientOptions = {
  ip: string;
  port: number;
  token: string;
};

const parsePositiveInteger = (
  value: string | undefined,
  fallback: number
): number => {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parsePort = (value: string | undefined, fallback: number): number =>
  parsePositiveInteger(value, fallback);

const integrationOptions: ControlClientOptions = {
  ip: runtimeEnv.EVENTDBX_TEST_IP ?? baseOptions.ip,
  port: parsePort(runtimeEnv.EVENTDBX_TEST_PORT, baseOptions.port),
  token: runtimeEnv.EVENTDBX_TEST_TOKEN ?? baseOptions.token,
};

const defaultSeedConcurrency = parsePositiveInteger(
  runtimeEnv.EVENTDBX_SEED_CONCURRENCY,
  8
);

type SeedResult = { success: true } | { success: false; reason: string };

const ensureEventdbxDataset = async (
  client: ReturnType<typeof createClient>,
  targetSize: number
): Promise<SeedResult> => {
  const limit = Math.max(1, Math.trunc(targetSize));

  // Check existing records
  const result = await client.select(
    aggregateType,
    formatAggregateId(targetSize),
    ["benchDataset"]
  );

  // If already exists, skip seeding
  if (result?.benchDataset === true) {
    return { success: true };
  }

  const concurrency = Math.max(1, Math.min(limit, defaultSeedConcurrency));
  let seedFailureReason: string | undefined;
  let nextIndex = 1;

  const takeNextIndex = () => {
    if (nextIndex > limit) {
      return undefined;
    }
    const current = nextIndex;
    nextIndex += 1;
    return current;
  };

  const seedAggregate = async (index: number) => {
    const aggregateId = formatAggregateId(index);
    const now = new Date().toISOString();
    try {
      await client.create(aggregateType, aggregateId, "created", {
        payload: {
          name: `Account ${aggregateId}`,
          field1: `value-${aggregateId}`,
          field2: index,
          archived: false,
          benchDataset: true,
          createdAt: now,
        },
      });
    } catch (error) {
      const message = toErrorMessage(error);
      if (/already exists/i.test(message) || /conflict/i.test(message)) {
        return;
      }
      throw message;
    }
  };

  const worker = async () => {
    while (true) {
      if (seedFailureReason) {
        return;
      }

      const index = takeNextIndex();
      if (index === undefined) {
        return;
      }

      try {
        await seedAggregate(index);
      } catch (error) {
        seedFailureReason = toErrorMessage(error);
        return;
      }
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => worker()));

  if (seedFailureReason) {
    return { success: false, reason: seedFailureReason };
  }

  return { success: true };
};

test("benchmarks eventdbx control operations", async (t) => {
  const client = createClient(integrationOptions);

  try {
    await client.connect();
  } catch (error) {
    t.log(`Skipping benchmark – unable to connect: ${toErrorMessage(error)}`);
    t.pass();
    return;
  }

  try {
    for (const [datasetIndex, size] of datasetSizes.entries()) {
      const seedResult = await ensureEventdbxDataset(client, size);
      if (!seedResult.success) {
        t.log(
          `Skipping EventDBX benchmark – unable to seed dataset: ${seedResult.reason}`
        );
        t.pass();
        return;
      }
      logDatasetPreparation(t, "EventDBX control", size, datasetIndex);
      const datasetLabel = `test${datasetIndex + 1}`;
      const pageSize = Math.max(1, Math.min(listLimit, Number(size)));
      const eventWindow = Math.max(1, Math.min(eventsLimit, Number(size)));
      const bench = createBench(`EventDBX ${datasetLabel}`);

      const aggregateIds = Array.from(
        { length: Math.max(1, Math.trunc(size)) },
        (_, index) => formatAggregateId(index + 1)
      );

      let aggregateCursor = 0;
      const pickAggregateId = () => {
        const aggregateId = aggregateIds[aggregateCursor];
        aggregateCursor = (aggregateCursor + 1) % aggregateIds.length;
        return aggregateId;
      };

      let listCursor: string | undefined;
      const aggregateEventCursors = new Map<string, string | undefined>();

      const benchOperations: Array<[string, AsyncOperation]> = [
        [
          "list",
          async () => {
            const result = await client.list(aggregateType, {
              take: pageSize,
              cursor: listCursor,
            });
            listCursor = result.nextCursor ?? undefined;
          },
        ],
        ["get", () => client.get(aggregateType, pickAggregateId())],
        [
          "select",
          () =>
            client.select(
              aggregateType,
              pickAggregateId(),
              Array.from(projectionFields)
            ),
        ],
        [
          "events",
          async () => {
            const aggregateId = pickAggregateId();
            const cursor = aggregateEventCursors.get(aggregateId);
            const result = await client.events(aggregateType, aggregateId, {
              take: eventWindow,
              cursor,
            });
            aggregateEventCursors.set(
              aggregateId,
              result.nextCursor ?? undefined
            );
          },
        ],
        [
          "apply",
          () =>
            client.apply(aggregateType, pickAggregateId(), "BenchApplied", {
              payload: { marker: "apply", at: new Date().toISOString() },
            }),
        ],
        [
          "create",
          () =>
            client.create(aggregateType, `bench-${randomUUID()}`, "Created", {
              payload: {
                name: "Benchmark Account",
                createdAt: new Date().toISOString(),
                field1: "value-bench",
                field2: 0,
              },
            }),
        ],
        [
          "archive",
          () =>
            client.archive(aggregateType, pickAggregateId(), {
              comment: "benchmark archive",
            }),
        ],
        [
          "restore",
          () =>
            client.restore(aggregateType, pickAggregateId(), {
              comment: "benchmark restore",
            }),
        ],
        [
          "patch",
          () =>
            client.patch(
              aggregateType,
              pickAggregateId(),
              "Patched",
              [{ op: "replace", path: "/name", value: "New Name" }],
              { note: "benchmark patch" }
            ),
        ],
      ];

      const operations = filterBenchOperations(benchOperations);

      logRunModeNotice(
        t,
        "EventDBX",
        datasetIndex,
        benchOperations.length,
        operations.length
      );

      if (operations.length === 0) {
        t.log(
          `No operations enabled for EventDBX dataset ${datasetLabel} with run mode "${benchRunMode}". Skipping.`
        );
        continue;
      }

      for (const [label, action] of operations) {
        addBenchTask(bench, label, () => runOperation(label, action));
      }

      await bench.run();

      validateBenchTasks(t, bench);
      t.log(summarizeBench(bench, `EventDBX ${datasetLabel}`));
      t.log("");
    }
  } finally {
    await client.disconnect().catch(() => {});
  }
});
