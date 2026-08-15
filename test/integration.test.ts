import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { GatewayDatabase } from "../src/database.js";
import { GatewayService } from "../src/gateway.js";
import { buildServer } from "../src/server.js";
import type { GatewayConfig, JsonObject } from "../src/types.js";

interface FakeOptions {
  id: string;
  deviceName: string;
  delayMs: number;
  online?: boolean;
  batchNode?: boolean;
  stochasticBatchNode?: boolean;
  failBatches?: boolean;
}

class FakeComfyUi {
  readonly server: Server;
  url = "";
  online: boolean;
  delayMs: number;
  readonly running = new Set<string>();
  readonly histories = new Map<string, JsonObject>();
  readonly submitCounts = new Map<string, number>();
  readonly submissions = new Map<string, JsonObject>();
  readonly managerCalls: string[] = [];
  activeLoras = new Set(["characters/test.safetensors"]);

  constructor(readonly options: FakeOptions) {
    this.online = options.online ?? true;
    this.delayMs = options.delayMs;
    this.server = createServer((request, response) => { void this.handle(request, response); });
  }

  async start(): Promise<void> {
    await new Promise<void>((resolvePromise) => this.server.listen(0, "127.0.0.1", resolvePromise));
    const address = this.server.address();
    if (!address || typeof address === "string") throw new Error("fake server did not bind TCP");
    this.url = `http://127.0.0.1:${address.port}`;
  }

  async stop(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>((resolvePromise, reject) => this.server.close((error) => error ? reject(error) : resolvePromise()));
  }

  private json(response: ServerResponse, status: number, body: unknown): void {
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  }

  private async read(request: IncomingMessage): Promise<Buffer> {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    return Buffer.concat(chunks);
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (!this.online) { this.json(response, 503, { error: "offline" }); return; }
    const url = new URL(request.url ?? "/", "http://fake.invalid");
    if (request.method === "GET" && url.pathname === "/system_stats") {
      this.json(response, 200, { system: { os: "fake" }, devices: [{ name: this.options.deviceName, type: "cuda", index: 0 }] }); return;
    }
    if (request.method === "GET" && url.pathname === "/object_info/GatewayMultiSeedNoise") {
      this.json(response, 200, this.options.batchNode ? { GatewayMultiSeedNoise: { input: { required: { seeds: ["STRING", {}] } } } } : {}); return;
    }
    if (request.method === "GET" && url.pathname === "/object_info/GatewayMultiSeedStochasticSampler") {
      this.json(response, 200, this.options.stochasticBatchNode ? { GatewayMultiSeedStochasticSampler: { input: { required: { seeds: ["STRING", {}] } } } } : {}); return;
    }
    if (request.method === "POST" && url.pathname === "/prompt") {
      const body = JSON.parse((await this.read(request)).toString("utf8")) as JsonObject & { prompt_id: string };
      const id = body.prompt_id;
      this.submitCounts.set(id, (this.submitCounts.get(id) ?? 0) + 1);
      this.submissions.set(id, body);
      let batchSeeds: string[] = [];
      const prompt = body.prompt;
      if (prompt && typeof prompt === "object" && !Array.isArray(prompt)) {
        for (const value of Object.values(prompt)) {
          if (!value || typeof value !== "object" || Array.isArray(value) || value.class_type !== "GatewayMultiSeedNoise") continue;
          const inputs = value.inputs;
          if (!inputs || typeof inputs !== "object" || Array.isArray(inputs) || typeof inputs.seeds !== "string") continue;
          batchSeeds = JSON.parse(inputs.seeds) as string[];
        }
        for (const value of Object.values(prompt)) {
          if (!value || typeof value !== "object" || Array.isArray(value) || value.class_type !== "GatewayMultiSeedStochasticSampler") continue;
          const inputs = value.inputs;
          if (!inputs || typeof inputs !== "object" || Array.isArray(inputs) || typeof inputs.seeds !== "string") continue;
          batchSeeds = JSON.parse(inputs.seeds) as string[];
        }
      }
      this.running.add(id);
      setTimeout(() => {
        if (!this.running.delete(id)) return;
        const failed = this.options.failBatches === true && batchSeeds.length > 1;
        const images = (batchSeeds.length > 0 ? batchSeeds : [id]).map((seed, index) => ({ filename: `${id}-${index}-${seed}.png`, subfolder: "", type: "output" }));
        this.histories.set(id, {
          [id]: {
            outputs: failed ? {} : { [batchSeeds.length > 0 ? "15" : "9"]: { images } },
            status: { status_str: failed ? "error" : "success", completed: true },
          },
        });
      }, this.delayMs).unref();
      this.json(response, 200, { prompt_id: id, number: 1, node_errors: {} }); return;
    }
    if (request.method === "GET" && url.pathname.startsWith("/history/")) {
      const id = decodeURIComponent(url.pathname.slice("/history/".length));
      this.json(response, 200, this.histories.get(id) ?? {}); return;
    }
    if (request.method === "GET" && url.pathname === "/queue") {
      this.json(response, 200, { queue_running: [...this.running].map((id) => [1, id, {}, {}]), queue_pending: [] }); return;
    }
    if (request.method === "GET" && url.pathname === "/view") {
      const filename = url.searchParams.get("filename") ?? "";
      response.writeHead(200, { "content-type": "image/png" }); response.end(Buffer.from(`fake-image:${this.options.id}:${filename}`)); return;
    }
    if (request.method === "POST" && url.pathname === "/interrupt") {
      const body = JSON.parse((await this.read(request)).toString("utf8")) as { prompt_id: string };
      this.running.delete(body.prompt_id);
      this.histories.set(body.prompt_id, { [body.prompt_id]: { outputs: {}, status: { status_str: "error", completed: true } } });
      this.json(response, 200, { success: true }); return;
    }
    if (request.method === "GET" && url.pathname === "/api/lm/loras/scan") {
      this.managerCalls.push(`${request.method} ${url.pathname}?${url.searchParams.toString()}`);
      this.json(response, 200, { status: "success", message: "Lora scan completed" }); return;
    }
    if (request.method === "GET" && url.pathname === "/api/lm/loras/list") {
      const items = [...this.activeLoras].map((relativePath) => {
        const slash = relativePath.lastIndexOf("/");
        const folder = slash >= 0 ? relativePath.slice(0, slash) : "";
        const filename = slash >= 0 ? relativePath.slice(slash + 1) : relativePath;
        return { file_name: filename.replace(/\.safetensors$/, ""), folder, file_path: `/app/ComfyUI/models/loras/${relativePath}` };
      });
      this.json(response, 200, { items, page: 1, page_size: 100, total: items.length, total_pages: 1 }); return;
    }
    if (request.method === "GET" && ["/api/lm/loras/excluded", "/api/lm/loras/roots", "/api/lm/loras/metadata", "/api/lm/downloads/queue", "/api/lm/downloads/history"].includes(url.pathname)) {
      this.managerCalls.push(`${request.method} ${url.pathname}`); this.json(response, 200, url.pathname.endsWith("roots") ? { success: true, roots: ["loras"] } : { success: true, items: [] }); return;
    }
    if (request.method === "POST" && ["/api/lm/loras/exclude", "/api/lm/loras/unexclude", "/api/lm/download-model"].includes(url.pathname)) {
      const body = JSON.parse((await this.read(request)).toString("utf8") || "{}") as { file_path?: string; download_id?: string };
      this.managerCalls.push(`${request.method} ${url.pathname}`);
      if (url.pathname.endsWith("exclude") && !url.pathname.endsWith("unexclude") && body.file_path) this.activeLoras.delete(body.file_path);
      if (url.pathname.endsWith("unexclude") && body.file_path) this.activeLoras.add(body.file_path);
      this.json(response, 200, url.pathname.endsWith("download-model") ? { success: true, download_id: body.download_id ?? "download" } : { success: true }); return;
    }
    this.managerCalls.push(`${request.method} ${url.pathname}`);
    this.json(response, 404, { error: "not found" });
  }
}

interface RunningGateway { database: GatewayDatabase; gateway: GatewayService; app: Awaited<ReturnType<typeof buildServer>>; url: string }

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

function config(root: string, primary: FakeComfyUi, fast: FakeComfyUi, overrides: Partial<GatewayConfig["scheduler"]> = {}): GatewayConfig {
  return {
    version: 1, listen: { host: "127.0.0.1", port: 19189 }, databasePath: join(root, "gateway.sqlite"), outputDirectory: join(root, "outputs"),
    workers: [
      { id: "a3000-control", url: primary.url, expectedDeviceName: primary.options.deviceName, secondsPerImage: 0.266, capabilities: ["default", "fast-sdxl"], required: true, primary: true, enabled: true },
      { id: "rtx3090-worker", url: fast.url, expectedDeviceName: fast.options.deviceName, secondsPerImage: 0.12, capabilities: ["default", "fast-sdxl"], required: false, primary: false, enabled: true },
    ],
    timeouts: { workerRequestMs: 500, historyPollMs: 20, maxJobRuntimeMs: 5000, catalogDrainMs: 2000 },
    limits: { maxPromptBytes: 1024 * 1024, maxOutputBytes: 1024 * 1024, maxOutputsPerJob: 8, maxQueuedJobs: 100 },
    scheduler: { ewmaAlpha: overrides.ewmaAlpha ?? 0.5, ewmaMinSamples: overrides.ewmaMinSamples ?? 10, agingSeconds: overrides.agingSeconds ?? 30, tieEpsilonMs: overrides.tieEpsilonMs ?? 5 },
    batching: { enabled: false, mergeWindowMs: 20, workers: {} },
    catalog: { refreshOnStart: true, retryMs: 50, pageSize: 100 },
    retention: { maxAgeHours: 24, maxTotalBytes: 1024 * 1024 * 1024, minAgeHours: 1, sweepIntervalMs: 60000 },
    auth: { generationToken: "generation-test", managementToken: "management-test" },
  };
}

async function startGateway(gatewayConfig: GatewayConfig): Promise<RunningGateway> {
  const database = new GatewayDatabase(gatewayConfig.databasePath);
  const gateway = new GatewayService(gatewayConfig, database);
  await gateway.start();
  const app = await buildServer(gateway, gatewayConfig);
  const url = await app.listen({ host: "127.0.0.1", port: 0 });
  return { database, gateway, app, url };
}

async function stopGateway(running: RunningGateway): Promise<void> {
  await running.gateway.stop();
  running.app.server.closeAllConnections();
  await running.app.close();
  running.database.close();
}

async function gatewayFetch(running: RunningGateway, path: string, init: RequestInit = {}, management = false): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${management ? "management-test" : "generation-test"}`);
  if (init.body && !headers.has("content-type")) headers.set("content-type", "application/json");
  return fetch(`${running.url}${path}`, { ...init, headers });
}

async function submit(running: RunningGateway, prompt: Record<string, unknown> = { "1": { class_type: "SaveImage", inputs: {} } }): Promise<string> {
  const response = await gatewayFetch(running, "/prompt", { method: "POST", body: JSON.stringify({ prompt, client_id: "test" }) });
  expect(response.status).toBe(200);
  return ((await response.json()) as { prompt_id: string }).prompt_id;
}

function batchPrompt(seed: number, text = "same prompt"): Record<string, unknown> {
  return {
    "1": { class_type: "CheckpointLoaderSimple", inputs: { ckpt_name: "model.safetensors" } },
    "5": { class_type: "CLIPTextEncode", inputs: { text, clip: ["1", 1] } },
    "6": { class_type: "CLIPTextEncode", inputs: { text: "negative", clip: ["1", 1] } },
    "7": { class_type: "EmptyLatentImage", inputs: { width: 1024, height: 1536, batch_size: 1 } },
    "13": { class_type: "KSampler", inputs: { seed, steps: 28, cfg: 5, sampler_name: "euler", scheduler: "normal", denoise: 1, model: ["1", 0], positive: ["5", 0], negative: ["6", 0], latent_image: ["7", 0] } },
    "14": { class_type: "VAEDecode", inputs: { samples: ["13", 0], vae: ["1", 2] } },
    "15": { class_type: "SaveImage", inputs: { filename_prefix: "plugin-prefix", images: ["14", 0] } },
  };
}

function animaBatchPrompt(seed: number, samplerName = "euler"): Record<string, unknown> {
  return {
    "1": { class_type: "UNETLoader", inputs: { unet_name: "anima-base-v1.0.safetensors", weight_dtype: "default" } },
    "2": { class_type: "CLIPLoader", inputs: { clip_name: "qwen_3_06b_base.safetensors", type: "stable_diffusion", device: "default" } },
    "3": { class_type: "VAELoader", inputs: { vae_name: "qwen_image_vae.safetensors" } },
    "5": { class_type: "CLIPTextEncode", inputs: { text: "same", clip: ["2", 0] } },
    "6": { class_type: "CLIPTextEncode", inputs: { text: "bad", clip: ["2", 0] } },
    "7": { class_type: "EmptyLatentImage", inputs: { width: 1024, height: 1024, batch_size: 1 } },
    "13": { class_type: "KSampler", inputs: { seed, steps: 35, cfg: 4.5, sampler_name: samplerName, scheduler: "sgm_uniform", denoise: 1, model: ["1", 0], positive: ["5", 0], negative: ["6", 0], latent_image: ["7", 0] } },
    "14": { class_type: "VAEDecode", inputs: { samples: ["13", 0], vae: ["3", 0] } },
    "15": { class_type: "SaveImage", inputs: { filename_prefix: "anima", images: ["14", 0] } },
  };
}

async function waitFor<T>(probe: () => Promise<T | undefined>, timeoutMs = 5000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await probe();
    if (result !== undefined) return result;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 20));
  }
  throw new Error("timed out waiting for condition");
}

async function terminalHistory(running: RunningGateway, id: string): Promise<Record<string, unknown>> {
  return waitFor(async () => {
    const response = await gatewayFetch(running, `/history/${id}`);
    const body = await response.json() as Record<string, unknown>;
    return body[id] ? body : undefined;
  });
}

async function fixture(primaryOnline = true, fastOnline = true): Promise<{ root: string; primary: FakeComfyUi; fast: FakeComfyUi; running: RunningGateway }> {
  const root = await mkdtemp(join(tmpdir(), "gateway-ts-test-"));
  const primary = new FakeComfyUi({ id: "a3000", deviceName: "cuda:0 NVIDIA RTX A3000 Laptop GPU : cudaMallocAsync", delayMs: 266, online: primaryOnline });
  const fast = new FakeComfyUi({ id: "3090", deviceName: "cuda:0 NVIDIA GeForce RTX 3090 : cudaMallocAsync", delayMs: 120, online: fastOnline });
  await primary.start(); await fast.start();
  const running = await startGateway(config(root, primary, fast));
  cleanups.push(async () => { await stopGateway(running); await primary.stop(); await fast.stop(); await rm(root, { recursive: true, force: true }); });
  return { root, primary, fast, running };
}

async function batchFixture(options: { batchNode?: boolean; stochasticBatchNode?: boolean; failBatches?: boolean; delayMs?: number } = {}): Promise<{ root: string; primary: FakeComfyUi; fast: FakeComfyUi; running: RunningGateway; gatewayConfig: GatewayConfig }> {
  const root = await mkdtemp(join(tmpdir(), "gateway-ts-batch-"));
  const primary = new FakeComfyUi({
    id: "a3000", deviceName: "cuda:0 NVIDIA RTX A3000 Laptop GPU : cudaMallocAsync",
    delayMs: options.delayMs ?? 80, batchNode: options.batchNode ?? true, stochasticBatchNode: options.stochasticBatchNode ?? true, failBatches: options.failBatches ?? false,
  });
  const fast = new FakeComfyUi({ id: "3090", deviceName: "cuda:0 NVIDIA GeForce RTX 3090 : cudaMallocAsync", delayMs: 40 });
  await primary.start(); await fast.start();
  const gatewayConfig = config(root, primary, fast);
  gatewayConfig.workers[1]!.enabled = false;
  gatewayConfig.batching = {
    enabled: true,
    mergeWindowMs: 20,
    workers: { "a3000-control": { default: 2, "fast-sdxl": 2 } },
  };
  const running = await startGateway(gatewayConfig);
  cleanups.push(async () => { await stopGateway(running); await primary.stop(); await fast.stop(); await rm(root, { recursive: true, force: true }); });
  return { root, primary, fast, running, gatewayConfig };
}

describe("ComfyUI gateway black-box flow", () => {
  it("coalesces Anima UNETLoader euler/sgm_uniform requests", async () => {
    const { running, primary } = await batchFixture();
    const ids = await Promise.all([submit(running, animaBatchPrompt(101)), submit(running, animaBatchPrompt(202))]);
    const histories = await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(1);
    const physical = [...primary.submissions.values()][0]!;
    const prompt = physical.prompt as JsonObject;
    expect(prompt["7"]).toMatchObject({ inputs: { batch_size: 2 } });
    expect(prompt["13"]).toMatchObject({ class_type: "SamplerCustomAdvanced" });
    expect(prompt["19"]).toMatchObject({ class_type: "BasicScheduler", inputs: { scheduler: "sgm_uniform" } });
    const firstJob = running.database.getJob(ids[0]!);
    expect(firstJob).toBeDefined();
    const execution = running.database.getExecution(firstJob!.backendPromptId);
    expect(execution).toMatchObject({ strategyId: "anima-euler-sgm", strategyVersion: 1, batchSize: 2 });
    expect(JSON.parse(execution!.batchPlanJson)).toMatchObject({ adapter_id: "anima-euler-sgm", adapter_version: 1 });
    expect((histories[0]![ids[0]!] as JsonObject).outputs).toBeTruthy();
    expect((histories[1]![ids[1]!] as JsonObject).outputs).toBeTruthy();
  });

  it("coalesces Anima er_sde requests while preserving guider links and SaveImage node ID", async () => {
    const { running, primary } = await batchFixture();
    const ids = await Promise.all([submit(running, animaBatchPrompt(101, "er_sde")), submit(running, animaBatchPrompt(202, "er_sde"))]);
    const histories = await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(1);
    const physical = [...primary.submissions.values()][0]!;
    const prompt = physical.prompt as JsonObject;
    expect(prompt["7"]).toMatchObject({ inputs: { batch_size: 2 } });
    expect(prompt["13"]).toMatchObject({ class_type: "GatewayMultiSeedStochasticSampler", inputs: { seeds: "[\"101\",\"202\"]" } });
    expect(prompt["15"]).toMatchObject({ class_type: "SaveImage", inputs: { images: ["14", 0] } });
    const guider = Object.values(prompt).find((value) => value && typeof value === "object" && !Array.isArray(value) && value.class_type === "CFGGuider") as JsonObject;
    expect(guider.inputs).toMatchObject({ positive: ["5", 0], negative: ["6", 0] });
    const firstJob = running.database.getJob(ids[0]!);
    expect(running.database.getExecution(firstJob!.backendPromptId)).toMatchObject({ strategyId: "anima-er-sde", strategyVersion: 1, batchSize: 2 });
    expect((histories[0]![ids[0]!] as JsonObject).outputs).toBeTruthy();
    expect((histories[1]![ids[1]!] as JsonObject).outputs).toBeTruthy();
  });

  it("keeps Anima er_sde singleton when the stochastic worker node is unavailable", async () => {
    const { running, primary } = await batchFixture({ stochasticBatchNode: false });
    const ids = await Promise.all([submit(running, animaBatchPrompt(101, "er_sde")), submit(running, animaBatchPrompt(202, "er_sde"))]);
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(2);
    expect([...primary.submissions.values()].every((submission) => {
      const prompt = submission.prompt as JsonObject;
      return !Object.values(prompt).some((value) => value && typeof value === "object" && !Array.isArray(value) && value.class_type === "GatewayMultiSeedStochasticSampler");
    })).toBe(true);
  });

  it("coalesces compatible plugin requests and preserves independent public histories and views", async () => {
    const { running, primary } = await batchFixture();
    const [first, second] = await Promise.all([
      submit(running, batchPrompt(101)),
      submit(running, batchPrompt(202)),
    ]);
    expect(first).not.toBe(second);
    const [firstHistory, secondHistory] = await Promise.all([terminalHistory(running, first), terminalHistory(running, second)]);
    expect(primary.submissions.size).toBe(1);

    const firstJob = await (await gatewayFetch(running, `/gateway/v1/jobs/${first}`)).json() as { backendPromptId: string };
    const secondJob = await (await gatewayFetch(running, `/gateway/v1/jobs/${second}`)).json() as { backendPromptId: string };
    expect(firstJob.backendPromptId).toBe(secondJob.backendPromptId);
    expect(firstJob.backendPromptId).not.toBe(first);
    const physical = primary.submissions.get(firstJob.backendPromptId)!;
    const physicalPrompt = physical.prompt as JsonObject;
    const noise = Object.values(physicalPrompt).find((value) => value && typeof value === "object" && !Array.isArray(value) && value.class_type === "GatewayMultiSeedNoise") as JsonObject;
    expect(JSON.parse((noise.inputs as JsonObject).seeds as string)).toEqual(["101", "202"]);
    expect((physicalPrompt["7"] as JsonObject).inputs).toMatchObject({ batch_size: 2 });
    expect((physicalPrompt["13"] as JsonObject).class_type).toBe("SamplerCustomAdvanced");

    const firstImages = ((firstHistory[first] as JsonObject).outputs as JsonObject)["15"] as JsonObject;
    const secondImages = ((secondHistory[second] as JsonObject).outputs as JsonObject)["15"] as JsonObject;
    expect(firstImages.images).toHaveLength(1);
    expect(secondImages.images).toHaveLength(1);
    const firstImage = (firstImages.images as JsonObject[])[0]!;
    const secondImage = (secondImages.images as JsonObject[])[0]!;
    expect(firstImage.subfolder).toBe(first);
    expect(secondImage.subfolder).toBe(second);
    const firstView = await gatewayFetch(running, `/view?${new URLSearchParams(firstImage as Record<string, string>).toString()}`);
    const secondView = await gatewayFetch(running, `/view?${new URLSearchParams(secondImage as Record<string, string>).toString()}`);
    expect(await firstView.text()).toContain("-0-101.png");
    expect(await secondView.text()).toContain("-1-202.png");

    const status = await (await gatewayFetch(running, "/gateway/v1/status")).json() as { workers: Array<{ id: string; batchCapable: boolean; maxBatchSize: number }>; queue: { pending: number; running_members: number } };
    expect(status.workers.find((worker) => worker.id === "a3000-control")).toMatchObject({ batchCapable: true, maxBatchSize: 2 });
    expect(status.queue).toMatchObject({ pending: 0, running_members: 0 });
  });

  it("does not merge requests with different prompts", async () => {
    const { running, primary } = await batchFixture();
    const ids = await Promise.all([
      submit(running, batchPrompt(101, "first prompt")),
      submit(running, batchPrompt(202, "second prompt")),
    ]);
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(2);
    expect([...primary.submissions.values()].every((submission) => {
      const prompt = submission.prompt as JsonObject;
      return !Object.values(prompt).some((value) => value && typeof value === "object" && !Array.isArray(value) && value.class_type === "GatewayMultiSeedNoise");
    })).toBe(true);
  });

  it("automatically falls back to singleton execution when the worker lacks the batch node", async () => {
    const { running, primary } = await batchFixture({ batchNode: false, stochasticBatchNode: false });
    const ids = await Promise.all([submit(running, batchPrompt(101)), submit(running, batchPrompt(202))]);
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(2);
    const status = await (await gatewayFetch(running, "/gateway/v1/status")).json() as { workers: Array<{ id: string; batchCapable: boolean }> };
    expect(status.workers.find((worker) => worker.id === "a3000-control")?.batchCapable).toBe(false);
  });

  it("retries physical batch failures as independent singleton jobs", async () => {
    const { running, primary } = await batchFixture({ failBatches: true });
    const ids = await Promise.all([submit(running, batchPrompt(101)), submit(running, batchPrompt(202))]);
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submissions.size).toBe(3);
    const jobs = await Promise.all(ids.map(async (id) => await (await gatewayFetch(running, `/gateway/v1/jobs/${id}`)).json() as { status: string; batchBlocked: boolean }));
    expect(jobs).toEqual([
      expect.objectContaining({ status: "succeeded", batchBlocked: true }),
      expect.objectContaining({ status: "succeeded", batchBlocked: true }),
    ]);
  });

  it("recovers an in-flight physical batch after restart without duplicate GPU submission", async () => {
    const root = await mkdtemp(join(tmpdir(), "gateway-ts-batch-restart-"));
    const primary = new FakeComfyUi({ id: "a3000", deviceName: "cuda:0 NVIDIA RTX A3000 Laptop GPU : cudaMallocAsync", delayMs: 500, batchNode: true });
    const fast = new FakeComfyUi({ id: "3090", deviceName: "cuda:0 NVIDIA GeForce RTX 3090 : cudaMallocAsync", delayMs: 100 });
    await primary.start(); await fast.start();
    const gatewayConfig = config(root, primary, fast);
    gatewayConfig.workers[1]!.enabled = false;
    gatewayConfig.batching = { enabled: true, mergeWindowMs: 20, workers: { "a3000-control": { default: 2 } } };
    let running = await startGateway(gatewayConfig);
    const ids = await Promise.all([submit(running, batchPrompt(101)), submit(running, batchPrompt(202))]);
    await waitFor(async () => primary.submissions.size === 1 ? true : undefined);
    const executionId = [...primary.submissions.keys()][0]!;
    await stopGateway(running);
    running = await startGateway(gatewayConfig);
    cleanups.push(async () => { await stopGateway(running); await primary.stop(); await fast.stop(); await rm(root, { recursive: true, force: true }); });
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    expect(primary.submitCounts.get(executionId)).toBe(1);
  });

  it("logically cancels one batch member without interrupting the other member", async () => {
    const { running, primary } = await batchFixture({ delayMs: 300 });
    const [first, second] = await Promise.all([submit(running, batchPrompt(101)), submit(running, batchPrompt(202))]);
    await waitFor(async () => primary.submissions.size === 1 ? true : undefined);
    const cancel = await gatewayFetch(running, `/gateway/v1/jobs/${first}/cancel`, { method: "POST" });
    expect(await cancel.json()).toMatchObject({ cancelled: true, state: "cancelled" });
    const firstHistory = await terminalHistory(running, first);
    expect(((firstHistory[first] as JsonObject).status as JsonObject).status_str).toBe("error");
    await terminalHistory(running, second);
    expect(primary.submissions.size).toBe(1);
    const jobs = await Promise.all([first, second].map(async (id) => await (await gatewayFetch(running, `/gateway/v1/jobs/${id}`)).json() as { status: string }));
    expect(jobs.map((job) => job.status)).toEqual(["cancelled", "succeeded"]);
  });

  it("lets whichever worker finishes first consume the next queued image", async () => {
    const { running } = await fixture();
    const ids = await Promise.all(Array.from({ length: 6 }, () => submit(running)));
    await Promise.all(ids.map((id) => terminalHistory(running, id)));
    const jobs: Array<{ number: number; workerId: string }> = [];
    for (const id of ids) {
      const response = await gatewayFetch(running, `/gateway/v1/jobs/${id}`);
      jobs.push((await response.json()) as { number: number; workerId: string });
    }
    const workers = jobs.sort((left, right) => left.number - right.number).map((job) => job.workerId);
    expect(workers).toEqual(["rtx3090-worker", "a3000-control", "rtx3090-worker", "rtx3090-worker", "a3000-control", "rtx3090-worker"]);
    const history = await terminalHistory(running, ids[0]!);
    const entry = history[ids[0]!] as { outputs: Record<string, { images: Array<{ filename: string; subfolder: string; type: string }> }> };
    const image = entry.outputs["9"]!.images[0]!;
    const view = await gatewayFetch(running, `/view?${new URLSearchParams(image).toString()}`);
    expect(view.status).toBe(200);
    expect(view.headers.get("content-type")).toContain("image/png");
    expect((await view.text()).startsWith("fake-image:")).toBe(true);
  });

  it("cancels queued work and routes an interrupt only to the leased worker", async () => {
    const { running } = await fixture();
    const [first, second, third, queued] = await Promise.all([submit(running), submit(running), submit(running), submit(running)]);
    await waitFor(async () => {
      const response = await gatewayFetch(running, `/gateway/v1/jobs/${queued}`);
      return (await response.json() as { status: string }).status === "queued" ? true : undefined;
    });
    const cancel = await gatewayFetch(running, `/gateway/v1/jobs/${queued}/cancel`, { method: "POST" });
    expect(cancel.status).toBe(200);
    expect((await cancel.json() as { state: string }).state).toBe("cancelled");
    const interrupt = await gatewayFetch(running, "/interrupt", { method: "POST", body: JSON.stringify({ prompt_id: first }) });
    expect(interrupt.status).toBe(200);
    await waitFor(async () => {
      const response = await gatewayFetch(running, `/gateway/v1/jobs/${first}`);
      const job = await response.json() as { status: string };
      return job.status === "cancelled" ? true : undefined;
    });
    await Promise.all([second, third].map((id) => terminalHistory(running, id)));
    const queuedJob = await (await gatewayFetch(running, `/gateway/v1/jobs/${queued}`)).json() as { status: string };
    expect(queuedJob.status).toBe("cancelled");
  });

  it("replays generation idempotency keys without submitting duplicate GPU work", async () => {
    const { running, primary, fast } = await fixture();
    const body = JSON.stringify({ prompt: { "1": { class_type: "SaveImage", inputs: {} } }, client_id: "same-client" });
    const first = await gatewayFetch(running, "/prompt", { method: "POST", headers: { "x-idempotency-key": "generation-1" }, body });
    const second = await gatewayFetch(running, "/prompt", { method: "POST", headers: { "x-idempotency-key": "generation-1" }, body });
    const firstId = (await first.json() as { prompt_id: string }).prompt_id;
    const secondId = (await second.json() as { prompt_id: string }).prompt_id;
    expect(secondId).toBe(firstId);
    await terminalHistory(running, firstId);
    expect((primary.submitCounts.get(firstId) ?? 0) + (fast.submitCounts.get(firstId) ?? 0)).toBe(1);
    const conflict = await gatewayFetch(running, "/prompt", { method: "POST", headers: { "x-idempotency-key": "generation-1" }, body: JSON.stringify({ prompt: { "2": { class_type: "SaveImage", inputs: {} } }, client_id: "same-client" }) });
    expect(conflict.status).toBe(409);
  });

  it("isolates an optional offline 3090 and restores it after probe plus catalog rebuild", async () => {
    const { running, fast } = await fixture(true, false);
    const fallback = await submit(running); await terminalHistory(running, fallback);
    expect((await (await gatewayFetch(running, `/gateway/v1/jobs/${fallback}`)).json() as { workerId: string }).workerId).toBe("a3000-control");
    fast.online = true;
    await waitFor(async () => {
      const body = await (await gatewayFetch(running, "/gateway/v1/workers")).json() as { workers: Array<{ id: string; ready: boolean }> };
      return body.workers.find((worker) => worker.id === "rtx3090-worker")?.ready ? true : undefined;
    });
    const restored = await submit(running); await terminalHistory(running, restored);
    expect((await (await gatewayFetch(running, `/gateway/v1/jobs/${restored}`)).json() as { workerId: string }).workerId).toBe("rtx3090-worker");
  });

  it("switches from static weight to measured EWMA after the configured sample threshold", async () => {
    const root = await mkdtemp(join(tmpdir(), "gateway-ts-ewma-"));
    const primary = new FakeComfyUi({ id: "a3000", deviceName: "cuda:0 NVIDIA RTX A3000 Laptop GPU : cudaMallocAsync", delayMs: 266 });
    const fast = new FakeComfyUi({ id: "3090", deviceName: "cuda:0 NVIDIA GeForce RTX 3090 : cudaMallocAsync", delayMs: 400 });
    await primary.start(); await fast.start();
    const gatewayConfig = config(root, primary, fast, { ewmaMinSamples: 1 });
    const running = await startGateway(gatewayConfig);
    cleanups.push(async () => { await stopGateway(running); await primary.stop(); await fast.stop(); await rm(root, { recursive: true, force: true }); });
    const sample = await submit(running); await terminalHistory(running, sample);
    expect((await (await gatewayFetch(running, `/gateway/v1/jobs/${sample}`)).json() as { workerId: string }).workerId).toBe("rtx3090-worker");
    const status = await (await gatewayFetch(running, "/gateway/v1/workers")).json() as { workers: Array<{ id: string; effectiveSecondsPerImage: number; ewmaSamples: number }> };
    const measured = status.workers.find((worker) => worker.id === "rtx3090-worker");
    expect(measured?.ewmaSamples).toBe(1);
    expect(measured?.effectiveSecondsPerImage).toBeGreaterThan(0.3);
    const shifted = await submit(running); await terminalHistory(running, shifted);
    expect((await (await gatewayFetch(running, `/gateway/v1/jobs/${shifted}`)).json() as { workerId: string }).workerId).toBe("a3000-control");
  });

  it("recovers an in-flight job after gateway restart without submitting it twice", async () => {
    const root = await mkdtemp(join(tmpdir(), "gateway-ts-restart-"));
    const primary = new FakeComfyUi({ id: "a3000", deviceName: "cuda:0 NVIDIA RTX A3000 Laptop GPU : cudaMallocAsync", delayMs: 500 });
    const fast = new FakeComfyUi({ id: "3090", deviceName: "cuda:0 NVIDIA GeForce RTX 3090 : cudaMallocAsync", delayMs: 500, online: false });
    await primary.start(); await fast.start();
    const gatewayConfig = config(root, primary, fast);
    let running = await startGateway(gatewayConfig);
    const id = await submit(running);
    await waitFor(async () => primary.submitCounts.get(id) === 1 ? true : undefined);
    await stopGateway(running);
    running = await startGateway(gatewayConfig);
    cleanups.push(async () => { await stopGateway(running); await primary.stop(); await fast.stop(); await rm(root, { recursive: true, force: true }); });
    await terminalHistory(running, id);
    expect(primary.submitCounts.get(id)).toBe(1);
  });

  it("enforces LoRA path safety and completes mutation only after both worker catalogs advance", async () => {
    const { running, primary, fast } = await fixture();
    const unsafePrompt = { "23": { class_type: "Lora Loader (LoraManager)", inputs: { loras: { __value__: [{ name: "../escape.safetensors", active: true }] } } } };
    const unsafe = await gatewayFetch(running, "/prompt", { method: "POST", body: JSON.stringify({ prompt: unsafePrompt }) });
    expect(unsafe.status).toBe(400);
    const unknownPrompt = { "23": { class_type: "Lora Loader (LoraManager)", inputs: { loras: { __value__: [{ name: "unknown.safetensors", active: true }] } } } };
    const unknown = await gatewayFetch(running, "/prompt", { method: "POST", body: JSON.stringify({ prompt: unknownPrompt }) });
    expect(unknown.status).toBe(400);
    const knownPrompt = { "23": { class_type: "Lora Loader (LoraManager)", inputs: { loras: { __value__: [{ name: "characters/test.safetensors", active: true }] } } } };
    expect((await gatewayFetch(running, "/prompt", { method: "POST", body: JSON.stringify({ prompt: knownPrompt }) })).status).toBe(200);

    primary.managerCalls.length = 0; fast.managerCalls.length = 0;
    const mutation = await gatewayFetch(running, "/api/lm/loras/exclude", { method: "POST", body: JSON.stringify({ file_path: "characters/test.safetensors" }) }, true);
    expect(mutation.status).toBe(200);
    expect(mutation.headers.get("x-comfy-gateway-catalog-revision")).toBe("1");
    expect(primary.managerCalls.some((call) => call === "POST /api/lm/loras/exclude")).toBe(true);
    expect(primary.managerCalls.some((call) => call.includes("full_rebuild=true"))).toBe(true);
    expect(fast.managerCalls.some((call) => call.includes("full_rebuild=true"))).toBe(true);
    const workers = await (await gatewayFetch(running, "/gateway/v1/workers")).json() as { workers: Array<{ appliedRevision: number }>; catalog_revision: number };
    expect(workers.catalog_revision).toBe(1);
    expect(workers.workers.every((worker) => worker.appliedRevision === 1)).toBe(true);
    const repeat = await gatewayFetch(running, "/api/lm/loras/exclude", { method: "POST", body: JSON.stringify({ file_path: "characters/test.safetensors" }) }, true);
    expect(repeat.status).toBe(200);
    expect(primary.managerCalls.filter((call) => call === "POST /api/lm/loras/exclude")).toHaveLength(2);
    const blocked = await gatewayFetch(running, "/api/lm/not-allowlisted", {}, true);
    expect(blocked.status).toBe(403);
  });

  it("refreshes the primary inventory once before rejecting an unknown LoRA", async () => {
    const { running, primary } = await fixture();
    const prompt = { "23": { class_type: "Lora Loader (LoraManager)", inputs: { loras: { __value__: [{ name: "late/downloaded.safetensors", active: true }] } } } };
    primary.activeLoras.add("late/downloaded.safetensors");

    const response = await gatewayFetch(running, "/prompt", {
      method: "POST",
      body: JSON.stringify({ prompt }),
    });

    expect(response.status).toBe(200);
  });
});
