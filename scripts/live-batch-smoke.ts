import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const gatewayUrl = (process.argv[2] ?? "http://127.0.0.1:19189").replace(/\/$/, "");
const workflowPath = process.argv[3];
if (!workflowPath) throw new Error("usage: tsx scripts/live-batch-smoke.ts <gateway-url> <workflow.json>");

const sourceWorkflow = JSON.parse(await readFile(workflowPath, "utf8")) as Record<string, { class_type?: string; inputs?: Record<string, unknown> }>;

function workflow(seed: number): Record<string, unknown> {
  const prompt = structuredClone(sourceWorkflow);
  const sampler = Object.values(prompt).find((node) => node.class_type === "KSampler");
  if (!sampler?.inputs) throw new Error("workflow has no KSampler");
  sampler.inputs.seed = seed;
  return prompt;
}

async function json(path: string, init?: RequestInit): Promise<Record<string, unknown>> {
  const response = await fetch(`${gatewayUrl}${path}`, init);
  const body = await response.json() as Record<string, unknown>;
  if (!response.ok) throw new Error(`${path} returned HTTP ${response.status}: ${JSON.stringify(body)}`);
  return body;
}

async function submit(seed: number): Promise<string> {
  const body = await json("/prompt", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: workflow(seed), client_id: `gateway-live-smoke-${seed}` }),
  });
  if (typeof body.prompt_id !== "string") throw new Error("gateway returned no prompt_id");
  return body.prompt_id;
}

async function waitFor<T>(probe: () => Promise<T | undefined>, timeoutMs = 180_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await probe();
    if (result !== undefined) return result;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 500));
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}

async function job(id: string): Promise<Record<string, unknown>> {
  return json(`/gateway/v1/jobs/${encodeURIComponent(id)}`);
}

async function terminal(id: string): Promise<Record<string, unknown>> {
  return waitFor(async () => {
    const history = await json(`/history/${encodeURIComponent(id)}`);
    return history[id] && typeof history[id] === "object" ? history : undefined;
  });
}

async function imageHash(id: string, history: Record<string, unknown>): Promise<{ hash: string; bytes: number }> {
  const entry = history[id] as Record<string, unknown>;
  const outputs = entry.outputs as Record<string, { images?: Array<Record<string, string>> }>;
  const images = Object.values(outputs).flatMap((output) => output.images ?? []);
  if (images.length !== 1) throw new Error(`public history ${id} contains ${images.length} images instead of one`);
  const response = await fetch(`${gatewayUrl}/view?${new URLSearchParams(images[0]!).toString()}`);
  if (!response.ok) throw new Error(`/view for ${id} returned HTTP ${response.status}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  return { hash: createHash("sha256").update(bytes).digest("hex"), bytes: bytes.byteLength };
}

const singletonStartedAt = Date.now();
const singletonId = await submit(8_150_001);
const singletonHistory = await terminal(singletonId);
const singletonElapsedMs = Date.now() - singletonStartedAt;
const singletonJob = await job(singletonId);
const singletonImage = await imageHash(singletonId, singletonHistory);
if (singletonJob.workerId !== "a3000-control") throw new Error(`singleton ran on ${String(singletonJob.workerId)}`);

const batchStartedAt = Date.now();
const batchIds = await Promise.all([submit(8_150_002), submit(8_150_003)]);
const leasedJobs = await waitFor(async () => {
  const values = await Promise.all(batchIds.map(job));
  return values.every((value) => typeof value.backendPromptId === "string" && value.backendPromptId !== "") ? values : undefined;
});
const backendIds = new Set(leasedJobs.map((value) => value.backendPromptId));
if (backendIds.size !== 1) throw new Error(`batch members use different backend executions: ${JSON.stringify([...backendIds])}`);
if (leasedJobs.some((value) => value.workerId !== "a3000-control")) throw new Error("batch did not run entirely on A3000");
const activeStatus = await json("/gateway/v1/status");
const batchHistories = await Promise.all(batchIds.map(terminal));
const batchElapsedMs = Date.now() - batchStartedAt;
const batchImages = await Promise.all(batchIds.map((id, index) => imageHash(id, batchHistories[index]!)));
if (new Set(batchImages.map((image) => image.hash)).size !== batchImages.length) throw new Error("batch members returned duplicate image content");

console.log(JSON.stringify({
  singleton: { id: singletonId, elapsed_ms: singletonElapsedMs, worker_id: singletonJob.workerId, image: singletonImage },
  batch: {
    member_ids: batchIds,
    execution_id: [...backendIds][0],
    elapsed_ms: batchElapsedMs,
    elapsed_per_image_ms: Math.round(batchElapsedMs / batchIds.length),
    under_plugin_timeout: batchElapsedMs < 120_000,
    images: batchImages,
    status_snapshot: activeStatus.batching,
  },
  throughput_gain: Number((singletonElapsedMs / (batchElapsedMs / batchIds.length)).toFixed(3)),
}, null, 2));
