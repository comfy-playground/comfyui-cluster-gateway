import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { parse } from "yaml";
import type { GatewayConfig, JsonObject, JsonValue, WorkerConfig } from "./types.js";

function objectAt(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${path} must be an object`);
  }
  return value as Record<string, unknown>;
}

function exact(value: Record<string, unknown>, keys: readonly string[], path: string): void {
  const allowed = new Set(keys);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length > 0) throw new Error(`${path} has unknown field(s): ${unknown.join(", ")}`);
}

function stringAt(value: unknown, path: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.trim() === "")) throw new Error(`${path} must be a string`);
  return value;
}

function boolAt(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${path} must be a boolean`);
  return value;
}

function numberAt(value: unknown, path: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < minimum) {
    throw new Error(`${path} must be a finite number >= ${minimum}`);
  }
  return value;
}

function integerAt(value: unknown, path: string, minimum = 0): number {
  const result = numberAt(value, path, minimum);
  if (!Number.isSafeInteger(result)) throw new Error(`${path} must be an integer`);
  return result;
}

function stringsAt(value: unknown, path: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || item === "")) {
    throw new Error(`${path} must be a string array`);
  }
  return [...new Set(value as string[])];
}

function workerAt(value: unknown, index: number): WorkerConfig {
  const path = `workers[${index}]`;
  const raw = objectAt(value, path);
  exact(raw, ["id", "url", "expected_device_name", "seconds_per_image", "capabilities", "required", "primary", "enabled"], path);
  const url = stringAt(raw.url, `${path}.url`).replace(/\/$/, "");
  const parsed = new URL(url);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error(`${path}.url must use http or https`);
  return {
    id: stringAt(raw.id, `${path}.id`),
    url,
    expectedDeviceName: stringAt(raw.expected_device_name, `${path}.expected_device_name`),
    secondsPerImage: numberAt(raw.seconds_per_image, `${path}.seconds_per_image`, Number.MIN_VALUE),
    capabilities: stringsAt(raw.capabilities, `${path}.capabilities`),
    required: boolAt(raw.required, `${path}.required`),
    primary: boolAt(raw.primary, `${path}.primary`),
    enabled: boolAt(raw.enabled, `${path}.enabled`),
  };
}

export function parseConfig(source: string, env: NodeJS.ProcessEnv = process.env): GatewayConfig {
  const document = parse(source) as unknown;
  const root = objectAt(document, "config");
  exact(root, ["version", "listen", "database_path", "output_directory", "workers", "timeouts", "limits", "scheduler", "catalog", "retention", "auth"], "config");
  if (root.version !== 1) throw new Error("config.version must be 1");

  const listen = objectAt(root.listen, "listen");
  exact(listen, ["host", "port"], "listen");
  const timeouts = objectAt(root.timeouts, "timeouts");
  exact(timeouts, ["worker_request_ms", "history_poll_ms", "max_job_runtime_ms", "catalog_drain_ms"], "timeouts");
  const limits = objectAt(root.limits, "limits");
  exact(limits, ["max_prompt_bytes", "max_output_bytes", "max_outputs_per_job", "max_queued_jobs"], "limits");
  const scheduler = objectAt(root.scheduler, "scheduler");
  exact(scheduler, ["ewma_alpha", "ewma_min_samples", "aging_seconds", "tie_epsilon_ms"], "scheduler");
  const catalog = objectAt(root.catalog, "catalog");
  exact(catalog, ["refresh_on_start", "retry_ms", "page_size"], "catalog");
  const retention = objectAt(root.retention, "retention");
  exact(retention, ["max_age_hours", "max_total_bytes", "min_age_hours", "sweep_interval_ms"], "retention");
  const auth = objectAt(root.auth, "auth");
  exact(auth, ["generation_token_env", "management_token_env"], "auth");
  if (!Array.isArray(root.workers) || root.workers.length === 0) throw new Error("workers must be a non-empty array");
  const workers = root.workers.map(workerAt);
  const ids = new Set<string>();
  for (const worker of workers) {
    if (ids.has(worker.id)) throw new Error(`duplicate worker id ${worker.id}`);
    ids.add(worker.id);
  }
  const primaries = workers.filter((worker) => worker.enabled && worker.primary);
  if (primaries.length !== 1) throw new Error("exactly one enabled worker must be primary");
  if (!primaries[0]?.required) throw new Error("the primary worker must be required");

  const generationTokenEnv = stringAt(auth.generation_token_env, "auth.generation_token_env", true);
  const managementTokenEnv = stringAt(auth.management_token_env, "auth.management_token_env", true);
  const token = (name: string, path: string): string => {
    if (name === "") return "";
    const value = env[name];
    if (!value) throw new Error(`${path} names environment variable ${name}, but it is empty`);
    return value;
  };
  const alpha = numberAt(scheduler.ewma_alpha, "scheduler.ewma_alpha", Number.MIN_VALUE);
  if (alpha > 1) throw new Error("scheduler.ewma_alpha must be <= 1");
  const maxAgeHours = numberAt(retention.max_age_hours, "retention.max_age_hours", 0);
  const minAgeHours = numberAt(retention.min_age_hours, "retention.min_age_hours", 0);
  if (minAgeHours > maxAgeHours) throw new Error("retention.min_age_hours must not exceed max_age_hours");

  return {
    version: 1,
    listen: { host: stringAt(listen.host, "listen.host"), port: integerAt(listen.port, "listen.port", 1) },
    databasePath: resolve(stringAt(root.database_path, "database_path")),
    outputDirectory: resolve(stringAt(root.output_directory, "output_directory")),
    workers,
    timeouts: {
      workerRequestMs: integerAt(timeouts.worker_request_ms, "timeouts.worker_request_ms", 1),
      historyPollMs: integerAt(timeouts.history_poll_ms, "timeouts.history_poll_ms", 10),
      maxJobRuntimeMs: integerAt(timeouts.max_job_runtime_ms, "timeouts.max_job_runtime_ms", 1),
      catalogDrainMs: integerAt(timeouts.catalog_drain_ms, "timeouts.catalog_drain_ms", 1),
    },
    limits: {
      maxPromptBytes: integerAt(limits.max_prompt_bytes, "limits.max_prompt_bytes", 1),
      maxOutputBytes: integerAt(limits.max_output_bytes, "limits.max_output_bytes", 1),
      maxOutputsPerJob: integerAt(limits.max_outputs_per_job, "limits.max_outputs_per_job", 1),
      maxQueuedJobs: integerAt(limits.max_queued_jobs, "limits.max_queued_jobs", 1),
    },
    scheduler: { ewmaAlpha: alpha, ewmaMinSamples: integerAt(scheduler.ewma_min_samples, "scheduler.ewma_min_samples", 1), agingSeconds: numberAt(scheduler.aging_seconds, "scheduler.aging_seconds", 0), tieEpsilonMs: integerAt(scheduler.tie_epsilon_ms, "scheduler.tie_epsilon_ms", 0) },
    catalog: { refreshOnStart: boolAt(catalog.refresh_on_start, "catalog.refresh_on_start"), retryMs: integerAt(catalog.retry_ms, "catalog.retry_ms", 10), pageSize: integerAt(catalog.page_size, "catalog.page_size", 1) },
    retention: { maxAgeHours, maxTotalBytes: integerAt(retention.max_total_bytes, "retention.max_total_bytes", 1), minAgeHours, sweepIntervalMs: integerAt(retention.sweep_interval_ms, "retention.sweep_interval_ms", 1000) },
    auth: { generationToken: token(generationTokenEnv, "auth.generation_token_env"), managementToken: token(managementTokenEnv, "auth.management_token_env") },
  };
}

export async function loadConfig(path = process.env.COMFYUI_GATEWAY_CONFIG ?? "./config.yaml"): Promise<GatewayConfig> {
  return parseConfig(await readFile(path, "utf8"));
}

export function asJsonObject(value: unknown): JsonObject {
  return objectAt(value, "JSON") as JsonObject;
}

export function isJsonValue(value: unknown): value is JsonValue {
  if (value === null || ["string", "number", "boolean"].includes(typeof value)) return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  return typeof value === "object" && Object.values(value as Record<string, unknown>).every(isJsonValue);
}
