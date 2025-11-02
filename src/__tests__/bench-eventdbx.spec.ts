import type { AsyncOperation } from "./bench-shared.js";
import {
  aggregateType,
  createBench,
  datasetSizes,
  eventsLimit,
  listLimit,
  logDatasetPreparation,
  addBenchTask,
  projectionFields,
  runOperation,
  sampleAggregateId,
  summarizeBench,
  toErrorMessage,
  validateBenchTasks,
} from "./bench-shared.js";

import test from "ava";
import { randomUUID } from "node:crypto";

import { createClient } from "eventdbxjs";

type RuntimeEnv = Record<string, string | undefined>;
type RuntimeContext = { process?: { env?: RuntimeEnv } };

const runtimeEnv: RuntimeEnv =
  (globalThis as RuntimeContext).process?.env ?? {};

const baseOptions = {
  ip: "127.0.0.1",
  port: 6363,
  token:
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6ImtleS0yMDI1MTEwMTExMjcyMCJ9.eyJpc3MiOiJldmVudGRieDovL3NlbGYiLCJhdWQiOiJldmVudGRieC1jbGllbnRzIiwic3ViIjoiY2xpOmJvb3RzdHJhcCIsImp0aSI6Ijk5Y2VkM2M5LTk2MTktNGYyNS04MWVjLWQzMDEwOWIxYmY5NSIsImlhdCI6MTc2MTk5NjQ0MCwiZ3JvdXAiOiJjbGkiLCJ1c2VyIjoicm9vdCIsImFjdGlvbnMiOlsiKi4qIl0sInJlc291cmNlcyI6WyIqIl0sImlzc3VlZF9ieSI6ImNsaS1ib290c3RyYXAiLCJsaW1pdHMiOnsid3JpdGVfZXZlbnRzIjpudWxsLCJrZWVwX2FsaXZlIjpmYWxzZX19.DvOizlvAYs71HxOLnF439NNrAZuZu_uNyPpyiLyebB7GjkftZwpGLkvSvtvYLymzbKVFP52qWeZ7sGTF-3-ZDw",
};

type ControlClientOptions = {
  ip: string;
  port: number;
  token: string;
};

const parsePort = (value: string | undefined, fallback: number): number => {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const integrationOptions: ControlClientOptions = {
  ip: runtimeEnv.EVENTDBX_TEST_IP ?? baseOptions.ip,
  port: parsePort(runtimeEnv.EVENTDBX_TEST_PORT, baseOptions.port),
  token: runtimeEnv.EVENTDBX_TEST_TOKEN ?? baseOptions.token,
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
      logDatasetPreparation(t, "EventDBX control", size, datasetIndex);
      const datasetLabel = `test${datasetIndex + 1}`;
      const pageSize = Math.max(1, Math.min(listLimit, Number(size)));
      const eventWindow = Math.max(1, Math.min(eventsLimit, Number(size)));
      const bench = createBench(`EventDBX ${datasetLabel}`);

      const operations: Array<[string, AsyncOperation]> = [
        ["list", () => client.list(undefined, { take: pageSize, skip: 0 })],
        ["get", () => client.get(aggregateType, sampleAggregateId)],
        [
          "select",
          () =>
            client.select(
              aggregateType,
              sampleAggregateId,
              Array.from(projectionFields)
            ),
        ],
        [
          "events",
          () =>
            client.events(aggregateType, sampleAggregateId, {
              skip: 0,
              take: eventWindow,
            }),
        ],
        [
          "apply",
          () =>
            client.apply(aggregateType, sampleAggregateId, "BenchApplied", {
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
            client.archive(aggregateType, sampleAggregateId, {
              comment: "benchmark archive",
            }),
        ],
        [
          "restore",
          () =>
            client.restore(aggregateType, sampleAggregateId, {
              comment: "benchmark restore",
            }),
        ],
        [
          "patch",
          () =>
            client.patch(
              aggregateType,
              sampleAggregateId,
              "Patched",
              [{ op: "replace", path: "/name", value: "New Name" }],
              { note: "benchmark patch" }
            ),
        ],
      ];

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
