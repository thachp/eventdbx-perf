import "dotenv/config";
import { Bench } from "tinybench";

export type AsyncOperation = () => Promise<unknown>;
type ExecutionContext = import("ava").ExecutionContext;
type BenchInstance = Bench;

export const aggregateType = "account";
export const listLimit = 100;
export const eventsLimit = 100;
export const projectionFields = ["state.field1", "state.field2"] as const;

// @TODO Dataset sizes to benchmark against
// Extend as needed for more comprehensive testing
export const datasetSizes = [1, 10_000, 100_000] as const;

export const formatAggregateId = (index: number) =>
  String(index).padStart(16, "0");

const aggregateCursors = new Map<number, number>();

export const sampleAggregateId = (size: number) => {
  const limit = Math.max(1, Math.trunc(size));
  const current = aggregateCursors.get(limit) ?? 0;
  const next = (current + 1) % limit;
  aggregateCursors.set(limit, next);
  return formatAggregateId(current + 1);
};

export const operationDetails: Record<string, string> = {
  list: "aggregate listing (latest x)",
  get: "load aggregate snapshot",
  select: "project selected fields",
  events: "read event stream (latest x)",
  apply: "append event",
  create: "create aggregate",
  archive: "archive aggregate",
  restore: "restore aggregate",
  patch: "apply JSON patch & record event",
};

export const formatDatasetLabel = (count: number) =>
  `${count.toLocaleString()} records`;

export type BenchRunMode = "all" | "read" | "write";

const normalizeRunMode = (
  value: string | undefined
): BenchRunMode | undefined => {
  if (!value) {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (
    normalized === "all" ||
    normalized === "both" ||
    normalized === "default"
  ) {
    return "all";
  }
  if (
    normalized === "read" ||
    normalized === "read-only" ||
    normalized === "readonly"
  ) {
    return "read";
  }
  if (
    normalized === "write" ||
    normalized === "write-only" ||
    normalized === "writeonly"
  ) {
    return "write";
  }
  return undefined;
};

export const benchRunMode: BenchRunMode =
  normalizeRunMode(process.env.BENCH_RUN_MODE) ??
  normalizeRunMode(process.env.BENCH_MODE) ??
  "all";

const readOperationLabels = new Set(["list", "get", "select", "events"]);
const writeOperationLabels = new Set([
  "apply",
  "create",
  "archive",
  "restore",
  "patch",
]);

export const isReadOperation = (label: string) =>
  readOperationLabels.has(label);
export const isWriteOperation = (label: string) =>
  writeOperationLabels.has(label);

export const isOperationEnabled = (label: string) => {
  switch (benchRunMode) {
    case "read":
      return isReadOperation(label);
    case "write":
      return isWriteOperation(label);
    default:
      return true;
  }
};

type FilterOptions = {
  onSkip?: (label: string) => void;
};

export const filterBenchOperations = <T>(
  operations: Array<[string, T]>,
  options?: FilterOptions
) => {
  const enabled: Array<[string, T]> = [];
  for (const [label, action] of operations) {
    if (isOperationEnabled(label)) {
      enabled.push([label, action]);
    } else {
      options?.onSkip?.(label);
    }
  }
  return enabled;
};

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

  // Tinybench reports latency statistics in milliseconds.
  if (value >= 1_000) {
    const seconds = value / 1_000;
    return `${seconds.toFixed(precisionFor(seconds))} s`;
  }
  if (value >= 1) {
    return `${value.toFixed(precisionFor(value))} ms`;
  }
  const micros = value * 1_000;
  if (micros >= 1) {
    return `${micros.toFixed(precisionFor(micros))} µs`;
  }
  const nanos = value * 1_000_000;
  return `${nanos.toFixed(precisionFor(nanos))} ns`;
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
