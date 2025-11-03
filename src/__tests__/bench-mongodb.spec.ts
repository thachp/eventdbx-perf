import test from "ava";
import { randomUUID } from "node:crypto";

import {
  addBenchTask,
  aggregateType,
  benchRunMode,
  createBench,
  datasetSizes,
  eventsLimit,
  formatAggregateId,
  formatDatasetLabel,
  filterBenchOperations,
  listLimit,
  loadOptionalModule,
  logDatasetPreparation,
  projectionFields,
  summarizeBench,
  toErrorMessage,
  validateBenchTasks,
} from "./bench-shared.js";

const ensureMongoDataset = async (
  eventsCollection: {
    countDocuments: (filter: Record<string, unknown>) => Promise<number>;
    insertMany: (
      docs: Array<Record<string, unknown>>,
      options?: Record<string, unknown>
    ) => Promise<void>;
    deleteMany: (filter: Record<string, unknown>) => Promise<unknown>;
  },
  aggregatesCollection: {
    countDocuments: (filter: Record<string, unknown>) => Promise<number>;
    insertMany: (
      docs: Array<Record<string, unknown>>,
      options?: Record<string, unknown>
    ) => Promise<void>;
    deleteMany: (filter: Record<string, unknown>) => Promise<unknown>;
  },
  targetSize: number
) => {
  await eventsCollection.deleteMany({
    category: aggregateType,
    benchDataset: true,
  });
  await aggregatesCollection.deleteMany({
    category: aggregateType,
    benchDataset: true,
  });

  const existing = await aggregatesCollection.countDocuments({
    category: aggregateType,
  });

  if (existing >= targetSize) {
    return existing;
  }

  const batchSize = 1_000;
  let inserted = existing;

  while (inserted < targetSize) {
    const upperBound = Math.min(inserted + batchSize, targetSize);
    const aggregateDocs: Array<Record<string, unknown>> = [];
    const eventDocs: Array<Record<string, unknown>> = [];

    for (let index = inserted; index < upperBound; index += 1) {
      const position = index + 1;
      const aggregateId = formatAggregateId(position);
      const name = `Account ${position}`;
      aggregateDocs.push({
        _id: aggregateId,
        aggregateId,
        category: aggregateType,
        state: {
          field1: `value-${aggregateId}`,
          field2: position,
          name,
          archived: false,
        },
        archived: false,
        index: position,
        benchDataset: true,
        updatedAt: new Date(),
      });
      eventDocs.push({
        aggregateId,
        category: aggregateType,
        eventType: "Created",
        payload: {
          field1: `value-${aggregateId}`,
          field2: position,
          name,
          version: 1,
        },
        benchDataset: true,
        createdAt: new Date(),
      });
    }

    await aggregatesCollection.insertMany(aggregateDocs, { ordered: false });
    await eventsCollection.insertMany(eventDocs, { ordered: false });
    inserted = upperBound;
  }

  return targetSize;
};

test("benchmarks mongodb operations", async (t) => {
  const uri =
    process.env.EVENTDBX_MONGO_URI ??
    process.env.MONGODB_URI ??
    process.env.MONGO_URI ??
    process.env.MONGO_URL ??
    process.env.MONGODB_URL ??
    "mongodb://bench:bench@localhost:27017";
  const dbName =
    process.env.EVENTDBX_MONGO_DB ??
    process.env.MONGO_DB ??
    process.env.MONGODB_DB ??
    process.env.MONGODB_DATABASE ??
    "bench";
  const collectionName =
    process.env.EVENTDBX_MONGO_COLLECTION ??
    process.env.MONGO_COLLECTION ??
    process.env.MONGODB_COLLECTION ??
    process.env.MONGO_COL ??
    process.env.MONGO_COLLECTION_NAME ??
    "events";
  const aggregateCollectionName =
    process.env.EVENTDBX_MONGO_AGGREGATE_COLLECTION ??
    process.env.MONGO_AGGREGATE_COLLECTION ??
    process.env.MONGODB_AGGREGATE_COLLECTION ??
    `${collectionName}_aggregates`;

  if (!uri || !dbName || !collectionName || !aggregateCollectionName) {
    t.log(
      "Skipping MongoDB benchmark – missing EVENTDBX_MONGO_* configuration"
    );
    t.pass();
    return;
  }

  const { module: mongoModule, error: mongoModuleError } =
    await loadOptionalModule<any>("mongodb");
  if (!mongoModule) {
    const message =
      mongoModuleError !== undefined
        ? `Skipping MongoDB benchmark – unable to load mongodb module: ${toErrorMessage(
            mongoModuleError
          )}`
        : "Skipping MongoDB benchmark – mongodb module not available";
    t.log(message);
    t.pass();
    return;
  }

  const client = new mongoModule.MongoClient(uri, {
    maxPoolSize: 20,
    serverSelectionTimeoutMS: 1_000,
  });

  try {
    await client.connect();
  } catch (error) {
    await client.close().catch(() => {});
    t.log(
      `Skipping MongoDB benchmark – unable to connect: ${toErrorMessage(error)}`
    );
    t.pass();
    return;
  }

  try {
    const database = client.db(dbName);
    const eventsCollection = database.collection(collectionName);
    const aggregatesCollection = database.collection(aggregateCollectionName);

    await eventsCollection.createIndex({ benchDataset: 1 }).catch(() => {});
    await eventsCollection.createIndex({ benchRun: 1 }).catch(() => {});
    await eventsCollection.createIndex({ aggregateId: 1 }).catch(() => {});
    await aggregatesCollection.createIndex({ category: 1 }).catch(() => {});
    await aggregatesCollection.createIndex({ benchDataset: 1 }).catch(() => {});
    await aggregatesCollection.createIndex({ aggregateId: 1 }).catch(() => {});
    await aggregatesCollection.createIndex({ index: 1 }).catch(() => {});

    await eventsCollection.deleteMany({
      benchRun: true,
      benchDataset: { $ne: true },
    });
    await aggregatesCollection.deleteMany({
      benchRun: true,
      benchDataset: { $ne: true },
    });

    for (const [datasetIndex, size] of datasetSizes.entries()) {
      await ensureMongoDataset(eventsCollection, aggregatesCollection, size);
      logDatasetPreparation(t, "MongoDB", size, datasetIndex);
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(
        size
      )})`;
      const pageSize = Math.max(1, Math.min(listLimit, Number(size)));

      const bench = createBench(`MongoDB ${datasetLabel}`);

      const aggregateDocs = await aggregatesCollection
        .find({ category: aggregateType })
        .sort({ index: 1 })
        .project({ aggregateId: 1, _id: 0 })
        .toArray();
      const aggregateIds = aggregateDocs
        .map((doc: { aggregateId?: unknown }) =>
          typeof doc.aggregateId === "string" ? doc.aggregateId : undefined
        )
        .filter(
          (value: string | undefined): value is string =>
            typeof value === "string"
        );
      if (aggregateIds.length === 0) {
        throw new Error("MongoDB aggregates collection is empty");
      }
      let aggregateCursor = 0;
      const pickAggregateId = () => {
        const aggregateId = aggregateIds[aggregateCursor];
        aggregateCursor = (aggregateCursor + 1) % aggregateIds.length;
        return aggregateId;
      };

      const operations: Array<[string, () => Promise<unknown>]> =
        filterBenchOperations(
          [
            [
              "list",
              () =>
                aggregatesCollection
                  .find({ category: aggregateType, archived: { $ne: true } })
                  .sort({ index: 1 })
                  .limit(pageSize)
                  .project({ aggregateId: 1 })
                  .toArray(),
            ],
            [
              "get",
              async () => {
                const aggregateId = pickAggregateId();
                await aggregatesCollection.findOne({
                  aggregateId,
                  category: aggregateType,
                });
              },
            ],
            [
              "select",
              async () => {
                const aggregateId = pickAggregateId();
                await aggregatesCollection.findOne(
                  { aggregateId, category: aggregateType },
                  {
                    projection: {
                      [projectionFields[0]]: 1,
                      [projectionFields[1]]: 1,
                      _id: 0,
                    },
                  }
                );
              },
            ],
            [
              "events",
              async () => {
                const aggregateId = pickAggregateId();
                await eventsCollection
                  .find({ aggregateId, category: aggregateType })
                  .sort({ createdAt: 1 })
                  .limit(eventsLimit)
                  .toArray();
              },
            ],
            [
              "apply",
              async () => {
                const aggregateId = pickAggregateId();
                await eventsCollection.insertOne({
                  aggregateId,
                  category: aggregateType,
                  eventType: "BenchApplied",
                  payload: { marker: "apply", at: new Date() },
                  benchRun: true,
                  createdAt: new Date(),
                });
              },
            ],
            [
              "create",
              async () => {
                const aggregateId = `bench-${randomUUID()}`;
                const now = new Date();
                await aggregatesCollection.insertOne({
                  _id: aggregateId,
                  aggregateId,
                  category: aggregateType,
                  state: {
                    field1: "value-bench",
                    field2: 0,
                    name: "Benchmark Account",
                    createdAt: now.toISOString(),
                    archived: false,
                  },
                  archived: false,
                  benchRun: true,
                  updatedAt: now,
                });
                await eventsCollection.insertOne({
                  aggregateId,
                  category: aggregateType,
                  eventType: "Created",
                  payload: {
                    name: "Benchmark Account",
                    createdAt: now.toISOString(),
                    field1: "value-bench",
                    field2: 0,
                  },
                  benchRun: true,
                  createdAt: now,
                });
              },
            ],
        [
          "archive",
          async () => {
            const aggregateId = pickAggregateId();
            const archivedAt = new Date();
            await aggregatesCollection.updateOne(
              { aggregateId, category: aggregateType },
              {
                $set: {
                  archived: true,
                  "state.archived": true,
                  updatedAt: archivedAt,
                },
              }
            );
            await eventsCollection.insertOne({
              aggregateId,
              category: aggregateType,
              eventType: "Archived",
              payload: { note: "benchmark archive", at: archivedAt },
              benchRun: true,
              createdAt: archivedAt,
            });
          },
        ],
        [
          "restore",
          async () => {
            const aggregateId = pickAggregateId();
            const restoredAt = new Date();
            await aggregatesCollection.updateOne(
              { aggregateId, category: aggregateType },
              {
                $set: {
                  archived: false,
                  "state.archived": false,
                  updatedAt: restoredAt,
                },
              }
            );
            await eventsCollection.insertOne({
              aggregateId,
              category: aggregateType,
              eventType: "Restored",
              payload: { note: "benchmark restore", at: restoredAt },
              benchRun: true,
              createdAt: restoredAt,
            });
          },
        ],
            [
              "patch",
              async () => {
                const aggregateId = pickAggregateId();
                await aggregatesCollection.updateOne(
                  { aggregateId, category: aggregateType },
                  {
                    $set: {
                      "state.name": "New Name",
                      updatedAt: new Date(),
                    },
                  }
                );
                await eventsCollection.insertOne({
                  aggregateId,
                  category: aggregateType,
                  eventType: "Patched",
                  payload: { name: "New Name", at: new Date() },
                  benchRun: true,
                  createdAt: new Date(),
                });
              },
            ],
          ],
          {
            onSkip: (label) =>
              t.log(
                `Skipping ${label} operation in mode "${benchRunMode}" for MongoDB benchmark`
              ),
          }
        );

      if (operations.length === 0) {
        t.log(
          `No operations enabled for MongoDB dataset ${datasetLabel} with run mode "${benchRunMode}". Skipping.`
        );
        continue;
      }

      for (const [label, action] of operations) {
        addBenchTask(bench, label, action);
      }

      await bench.run();

      validateBenchTasks(t, bench);
      t.log(summarizeBench(bench, `MongoDB ${datasetLabel}`));
      t.log("");
    }
  } finally {
    await client.close().catch(() => {});
  }
});
