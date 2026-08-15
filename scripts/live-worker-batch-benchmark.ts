import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createBatchAdapters } from "../src/batching/adapters.js";
import { BatchAdapterRegistry } from "../src/batching/registry.js";
import type { JsonObject, JsonValue, PromptEnvelope } from "../src/types.js";

const workerUrl = (process.argv[2] ?? "").replace(/\/$/, "");
const workflowPath = process.argv[3];
const batchSizes = (process.argv[4] ?? "1,2")
  .split(",")
  .map((value) => Number(value))
  .filter((value) => Number.isSafeInteger(value) && value > 0 && value <= 16);

if (!workerUrl || !workflowPath || batchSizes.length === 0) {
  throw new Error("usage: tsx scripts/live-worker-batch-benchmark.ts <worker-url> <workflow.json> <sizes-csv>");
}

const sourceWorkflow = JSON.parse(await readFile(workflowPath, "utf8")) as JsonObject;
const batchRegistry = new BatchAdapterRegistry(createBatchAdapters());
let supportedAdapterIds = new Set<string>();

function object(value: JsonValue | undefined): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
}

function envelope(seed: number, runId: string): PromptEnvelope {
  const prompt = structuredClone(sourceWorkflow);
  let sampler: JsonObject | undefined;
  let save: JsonObject | undefined;
  for (const value of Object.values(prompt)) {
    const promptNode = object(value);
    if (promptNode?.class_type === "KSampler") sampler = object(promptNode.inputs);
    if (promptNode?.class_type === "SaveImage") save = object(promptNode.inputs);
  }
  if (!sampler || !save) throw new Error("workflow must contain one KSampler and one SaveImage");
  sampler.seed = seed;
  save.filename_prefix = `gateway_batch_benchmark_${runId}`;
  return { prompt, client_id: `gateway-batch-benchmark-${runId}` };
}

function physicalEnvelope(batchSize: number, runId: string, seedBase: number): PromptEnvelope {
  if (batchSize === 1) return envelope(seedBase, runId);
  const candidates = Array.from({ length: batchSize }, (_, index) => {
    const assessment = batchRegistry.assess(envelope(seedBase + index, runId));
    if (!assessment.candidate) throw new Error(`workflow is not compatible with Gateway batch conversion: ${assessment.rejections.map((rejection) => rejection.kind === "reject" ? rejection.reason : "unknown").join(",")}`);
    return assessment.candidate;
  });
  const adapter = batchRegistry.get(candidates[0]!.adapterId);
  if (!adapter) throw new Error(`batch adapter ${candidates[0]!.adapterId} is unavailable`);
  if (!supportedAdapterIds.has(adapter.id)) throw new Error(`worker does not provide ${adapter.id} capability`);
  return adapter.merge(candidates, runId).envelope;
}

async function json(path: string, init?: RequestInit): Promise<JsonObject> {
  const response = await fetch(`${workerUrl}${path}`, init);
  const text = await response.text();
  let body: unknown;
  try { body = JSON.parse(text); }
  catch { throw new Error(`${path} returned invalid JSON (HTTP ${response.status}): ${text.slice(0, 1000)}`); }
  if (!response.ok) throw new Error(`${path} returned HTTP ${response.status}: ${JSON.stringify(body)}`);
  if (body === null || typeof body !== "object" || Array.isArray(body)) throw new Error(`${path} did not return an object`);
  return body as JsonObject;
}

interface MemorySample {
  vramTotal: number;
  vramFree: number;
  torchVramTotal: number;
  torchVramFree: number;
}

async function memorySample(): Promise<MemorySample | undefined> {
  const stats = await json("/system_stats");
  const devices = stats.devices;
  const device = Array.isArray(devices) ? object(devices[0]) : undefined;
  if (!device) return undefined;
  const values = [device.vram_total, device.vram_free, device.torch_vram_total, device.torch_vram_free];
  if (!values.every((value) => typeof value === "number")) return undefined;
  return {
    vramTotal: device.vram_total as number,
    vramFree: device.vram_free as number,
    torchVramTotal: device.torch_vram_total as number,
    torchVramFree: device.torch_vram_free as number,
  };
}

function images(entry: JsonObject): JsonObject[] {
  const outputs = object(entry.outputs) ?? {};
  const result: JsonObject[] = [];
  for (const outputValue of Object.values(outputs)) {
    const output = object(outputValue);
    if (!Array.isArray(output?.images)) continue;
    for (const image of output.images) {
      const imageObject = object(image);
      if (imageObject) result.push(imageObject);
    }
  }
  return result;
}

async function waitForHistory(promptId: string, timeoutMs = 15 * 60_000): Promise<JsonObject> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const history = await json(`/history/${encodeURIComponent(promptId)}`);
    const entry = object(history[promptId]);
    if (entry) return entry;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 500));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${promptId}`);
}

async function run(batchSize: number): Promise<JsonObject> {
  const runId = `${Date.now()}-${batchSize}-${randomUUID().slice(0, 8)}`;
  const promptId = randomUUID();
  // A fresh seed base prevents ComfyUI's node cache from turning repeated
  // benchmark invocations into cached no-op sampler runs.
  const seedBase = Date.now();
  const request = physicalEnvelope(batchSize, runId, seedBase);
  let sampling = true;
  let minimumFree = Number.POSITIVE_INFINITY;
  let minimumTorchFree = Number.POSITIVE_INFINITY;
  let maximumTorchReserved = 0;
  let totalVram = 0;
  const sampler = (async () => {
    while (sampling) {
      try {
        const sample = await memorySample();
        if (sample) {
          totalVram = sample.vramTotal;
          minimumFree = Math.min(minimumFree, sample.vramFree);
          minimumTorchFree = Math.min(minimumTorchFree, sample.torchVramFree);
          maximumTorchReserved = Math.max(maximumTorchReserved, sample.torchVramTotal);
        }
      } catch { /* The execution result is authoritative if stats sampling briefly fails. */ }
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
    }
  })();

  const startedAt = Date.now();
  try {
    const response = await json("/prompt", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...request, prompt_id: promptId }),
    });
    const backendPromptId = typeof response.prompt_id === "string" ? response.prompt_id : promptId;
    const entry = await waitForHistory(backendPromptId);
    const elapsedMs = Date.now() - startedAt;
    const status = object(entry.status) ?? {};
    const outputImages = images(entry);
    const succeeded = status.status_str === "success" && outputImages.length === batchSize;
    return {
      batch_size: batchSize,
      succeeded,
      prompt_id: backendPromptId,
      elapsed_ms: elapsedMs,
      elapsed_per_image_ms: Math.round(elapsedMs / batchSize),
      images: outputImages.length,
      unique_filenames: new Set(outputImages.map((image) => image.filename)).size,
      under_plugin_timeout: elapsedMs < 120_000,
      status,
      memory: {
        vram_total_mib: totalVram ? Math.round(totalVram / 1_048_576) : null,
        peak_vram_used_mib: Number.isFinite(minimumFree) ? Math.round((totalVram - minimumFree) / 1_048_576) : null,
        peak_torch_reserved_mib: Math.round(maximumTorchReserved / 1_048_576),
        minimum_torch_free_mib: Number.isFinite(minimumTorchFree) ? Math.round(minimumTorchFree / 1_048_576) : null,
      },
    };
  } finally {
    sampling = false;
    await sampler;
  }
}

const probe = batchRegistry.adapters.map(async (adapter) => {
  const supported = await Promise.all(adapter.requiredBackendNodes.map(async (node) => {
    try { return object((await json(`/object_info/${encodeURIComponent(node)}`))[node]) !== undefined; }
    catch { return false; }
  }));
  return { adapter, supported: supported.every(Boolean) };
});
const adapterSupport = (await Promise.all(probe)).filter((value) => value.supported);
if (adapterSupport.length === 0) throw new Error("worker provides no registered batch adapter capability");
supportedAdapterIds = new Set(adapterSupport.map((value) => value.adapter.id));

const results: JsonObject[] = [];
for (const batchSize of batchSizes) {
  try {
    const result = await run(batchSize);
    results.push(result);
    console.log(JSON.stringify(result));
    if (result.succeeded !== true) break;
  } catch (error) {
    const failure: JsonObject = { batch_size: batchSize, succeeded: false, error: error instanceof Error ? error.message : String(error) };
    results.push(failure);
    console.log(JSON.stringify(failure));
    break;
  }
  await new Promise((resolvePromise) => setTimeout(resolvePromise, 2_000));
}

console.log(JSON.stringify({ worker_url: workerUrl, workflow: workflowPath, results }, null, 2));
