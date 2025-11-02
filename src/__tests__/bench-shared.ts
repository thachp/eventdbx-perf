import "dotenv/config";
import { Bench } from "tinybench";

export type AsyncOperation = () => Promise<unknown>;
type ExecutionContext = import("ava").ExecutionContext;
type BenchInstance = Bench;

export const aggregateType = "account";
export const sampleAggregateId = "1";
export const listLimit = 10;
export const eventsLimit = 10;
export const projectionFields = ["state.field1", "state.field2"] as const;

export const datasetSizes = [1_000, 10_000, 100_000, 1_000_000] as const;

export const operationDetails: Record<string, string> = {
  list: "aggregate listing (latest 10)",
  get: "load aggregate snapshot",
  select: "project selected fields",
  events: "read event stream (latest 10)",
  apply: "append event",
  create: "create aggregate",
  archive: "archive aggregate",
  restore: "restore aggregate",
  patch: "apply JSON patch & record event",
};

export const formatDatasetLabel = (count: number) =>
  `${count.toLocaleString()} records`;

export const logDatasetPreparation = (
  t: ExecutionContext,
  backend: string,
  size: number,
  index: number
) => {
  t.log(
    `${backend} dataset ready: test${index + 1} (${formatDatasetLabel(size)})`
  );
};

export type OptionalModuleResult<T> =
  | {
      module: T;
      error?: undefined;
    }
  | {
      module?: undefined;
      error: unknown;
    };

export const loadOptionalModule = async <T = unknown>(
  specifier: string
): Promise<OptionalModuleResult<T>> => {
  let requireError: unknown;
  try {
    const { createRequire } = await import("node:module");
    const requireModule = createRequire(import.meta.url);
    return { module: requireModule(specifier) as T };
  } catch (error) {
    requireError = error;
  }

  try {
    return { module: (await import(specifier)) as T };
  } catch (importError) {
    return {
      error:
        importError ??
        requireError ??
        new Error(`Failed to load optional module: ${specifier}`),
    };
  }
};

let envInitialized = false;

export const loadEnv = async () => {
  if (envInitialized) {
    return;
  }
  envInitialized = true;
  const { module: dotenvModule } = await loadOptionalModule<{
    config: (options?: Record<string, unknown>) => unknown;
  }>("dotenv");
  if (dotenvModule && typeof dotenvModule.config === "function") {
    dotenvModule.config();
  }
};

await loadEnv();

export const addBenchTask = (
  bench: BenchInstance,
  label: string,
  action: AsyncOperation
) => {
  bench.add(label, async () => {
    await action();
  });
};

const precisionFor = (value: number) => {
  if (value >= 100) {
    return 0;
  }
  if (value >= 10) {
    return 1;
  }
  return 2;
};

export const formatLatency = (value: number) => {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  if (value >= 1_000_000) {
    const ms = value / 1_000_000;
    return `${ms.toFixed(precisionFor(ms))} ms`;
  }
  if (value >= 1_000) {
    const micros = value / 1_000;
    return `${micros.toFixed(precisionFor(micros))} µs`;
  }
  return `${value.toFixed(precisionFor(value))} ns`;
};

export const formatThroughput = (value: number) => {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  if (value >= 1_000_000_000) {
    const billions = value / 1_000_000_000;
    return `${billions.toFixed(precisionFor(billions))}B`;
  }
  if (value >= 1_000_000) {
    const millions = value / 1_000_000;
    return `${millions.toFixed(precisionFor(millions))}M`;
  }
  if (value >= 1_000) {
    const thousands = value / 1_000;
    return `${thousands.toFixed(precisionFor(thousands))}k`;
  }
  return value.toFixed(precisionFor(value));
};

export const formatRme = (value: number) => {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  return `${value.toFixed(2)}%`;
};

const formatGrid = (headers: string[], rows: string[][]) => {
  const allRows = [headers, ...rows];
  const columnWidths = headers.map((_, columnIndex) =>
    Math.max(...allRows.map((row) => (row[columnIndex] ?? "").length))
  );

  const renderRow = (row: string[]) =>
    row
      .map((cell, index) => {
        const width = columnWidths[index];
        return `${cell}`.padEnd(width, " ");
      })
      .join("  ");

  const separator = columnWidths.map((width) => "-".repeat(width)).join("  ");

  return [renderRow(headers), separator, ...rows.map(renderRow)].join("\n");
};

export const createBench = (name: string) =>
  new Bench({
    name,
    time: 150,
    warmupTime: 50,
    warmupIterations: 3,
    iterations: 5,
    throws: false,
  });

export const validateBenchTasks = (t: ExecutionContext, bench: Bench) => {
  for (const task of bench.tasks) {
    t.truthy(task.result, `${task.name} produced benchmark stats`);
    const latencySamples =
      task.result?.latency?.samples ?? task.result?.samples ?? [];
    if (latencySamples.length === 0) {
      t.log(`${task.name} did not record latency samples; treating as skipped`);
      continue;
    }
    t.true(latencySamples.length > 0, `${task.name} recorded latency samples`);
    t.falsy(task.result?.error, `${task.name} should not surface errors`);
  }
};

export const summarizeBench = (bench: Bench, label?: string) => {
  const rows = bench.tasks.map((task) => {
    const stats = task.result;

    if (!stats) {
      return [task.name, "no samples recorded", "no samples recorded"];
    }

    const throughputStats = stats.throughput;
    const latencyStats = stats.latency;
    const throughputMean = throughputStats?.mean ?? Number.NaN;
    const throughputRme = throughputStats?.rme ?? Number.NaN;
    const latencyMean = latencyStats?.mean ?? Number.NaN;
    const latencyRme = latencyStats?.rme ?? Number.NaN;

    const throughput = `${formatThroughput(throughputMean)} ±${formatRme(
      throughputRme
    )}`;
    const latency = `${formatLatency(latencyMean)} ±${formatRme(latencyRme)}`;

    return [task.name, throughput, latency];
  });

  const header = label ?? bench.name ?? "Bench summary";
  return [
    `${header}:`,
    formatGrid(["operation", "throughput (ops/s)", "latency"], rows),
  ].join("\n");
};

export const toErrorMessage = (error: unknown, fallback = "unknown error") => {
  if (error instanceof Error && typeof error.message === "string") {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return fallback;
  }
};

export const runOperation = async (label: string, action: AsyncOperation) => {
  try {
    await action();
  } catch (error) {
    const message = (error as Error).message ?? "";
    if (/client is not connected/i.test(message)) {
      throw new Error(
        `${label} failed because connection was lost: ${message}`
      );
    }
  }
};
