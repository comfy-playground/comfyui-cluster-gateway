import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

const [gatewayArgument = "http://127.0.0.1:19189", workflowPath, countArgument = "3"] = process.argv.slice(2);
if (!workflowPath) throw new Error("usage: npm run smoke -- <gateway-url> <workflow.json> [count]");
const gateway = new URL(gatewayArgument);
if (gateway.protocol !== "http:" || gateway.port !== "19189" || gateway.pathname !== "/") {
  throw new Error("smoke test is intentionally restricted to an HTTP gateway URL on port 19189");
}
const count = Number(countArgument);
if (!Number.isSafeInteger(count) || count < 1 || count > 12) throw new Error("count must be an integer from 1 to 12");
const template = JSON.parse(await readFile(workflowPath, "utf8")) as Record<string, unknown>;
const groupId = randomUUID();
const startedAt = Date.now();

function workflow(index: number): Record<string, unknown> {
  const copy = structuredClone(template);
  for (const nodeValue of Object.values(copy)) {
    if (nodeValue === null || typeof nodeValue !== "object" || Array.isArray(nodeValue)) continue;
    const node = nodeValue as { class_type?: unknown; inputs?: Record<string, unknown> };
    if (!node.inputs) continue;
    if (node.class_type === "KSampler" && typeof node.inputs.seed === "number") node.inputs.seed = Math.floor(Date.now() % 1_000_000_000) + index;
    if (node.class_type === "SaveImage") node.inputs.filename_prefix = `gateway-smoke/${groupId}/${index}`;
  }
  return copy;
}

async function submit(index: number): Promise<string> {
  const response = await fetch(new URL("/prompt", gateway), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      prompt: workflow(index), client_id: `gateway-smoke-${groupId}`,
      extra_data: { gateway: { group_id: groupId, sequence: index, count, profile: "fast-sdxl" } },
    }),
  });
  const body = await response.json() as { prompt_id?: string; error?: unknown };
  if (!response.ok || !body.prompt_id) throw new Error(`gateway submit ${index} failed: HTTP ${response.status} ${JSON.stringify(body)}`);
  return body.prompt_id;
}

async function waitForJob(id: string): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 10 * 60 * 1000;
  while (Date.now() < deadline) {
    const response = await fetch(new URL(`/history/${id}`, gateway));
    if (!response.ok) throw new Error(`history ${id} returned HTTP ${response.status}`);
    const history = await response.json() as Record<string, unknown>;
    if (history[id]) return history;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 500));
  }
  throw new Error(`job ${id} did not finish within 10 minutes`);
}

const ids = await Promise.all(Array.from({ length: count }, (_, offset) => submit(offset + 1)));
await Promise.all(ids.map(waitForJob));
const jobs = await Promise.all(ids.map(async (id) => {
  const response = await fetch(new URL(`/gateway/v1/jobs/${id}`, gateway));
  if (!response.ok) throw new Error(`job details ${id} returned HTTP ${response.status}`);
  return response.json() as Promise<{ id: string; number: number; status: string; workerId: string; submittedAtMs: number; completedAtMs: number }>;
}));
jobs.sort((left, right) => left.number - right.number);
process.stdout.write(`${JSON.stringify({ group_id: groupId, elapsed_ms: Date.now() - startedAt, jobs: jobs.map((job) => ({ id: job.id, number: job.number, status: job.status, worker_id: job.workerId, runtime_ms: job.completedAtMs - job.submittedAtMs })) }, null, 2)}\n`);
