import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { ReadStream } from "node:fs";
import { createReadStream } from "node:fs";
import type { GatewayDatabase } from "./database.js";
import { errorHistory } from "./database.js";
import { OutputStore } from "./output-store.js";
import { assignJobs, profileAndCost } from "./scheduler.js";
import type { CatalogOperation, GatewayConfig, JsonObject, JobRecord, JobStatus, PromptEnvelope, WorkerConfig, WorkerSnapshot, WorkerState } from "./types.js";
import { readResponseBytes, UpstreamHttpError, WorkerClient } from "./worker-client.js";

interface WorkerRuntime {
  config: WorkerConfig;
  client: WorkerClient;
  state: WorkerState;
  ready: boolean;
  currentJobId: string;
  appliedRevision: number;
  lastError: string;
  deviceName: string;
  ewmaMs: number;
  ewmaSamples: number;
  nextProbeAt: number;
}

export interface ManagerResult {
  operationId: string;
  status: number;
  contentType: string;
  body: Uint8Array;
  revision: number;
  optionalUnavailable: string[];
}

function message(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function sleep(milliseconds: number): Promise<void> { return new Promise((resolvePromise) => setTimeout(resolvePromise, milliseconds)); }

export class GatewayService extends EventEmitter {
  readonly workers = new Map<string, WorkerRuntime>();
  readonly outputStore: OutputStore;
  readonly primary: WorkerRuntime;
  private timer: NodeJS.Timeout | undefined;
  private retentionTimer: NodeJS.Timeout | undefined;
  private ticking = false;
  private stopped = false;
  private dispatchPaused = false;
  private managerSerial: Promise<void> = Promise.resolve();
  private inventory = new Set<string>();
  private inventoryReady = false;
  private inventoryRefresh: Promise<void> | undefined;

  constructor(readonly config: GatewayConfig, readonly database: GatewayDatabase) {
    super();
    for (const workerConfig of config.workers) {
      const stats = database.workerStats(workerConfig.id);
      this.workers.set(workerConfig.id, {
        config: workerConfig,
        client: new WorkerClient(workerConfig.url, config.timeouts.workerRequestMs),
        state: workerConfig.enabled ? "offline" : "disabled", ready: false, currentJobId: "", appliedRevision: 0,
        lastError: "not probed", deviceName: "", ewmaMs: stats.ewmaMs, ewmaSamples: stats.samples, nextProbeAt: 0,
      });
    }
    const primary = [...this.workers.values()].find((worker) => worker.config.enabled && worker.config.primary);
    if (!primary) throw new Error("primary worker missing");
    this.primary = primary;
    this.outputStore = new OutputStore(config.outputDirectory, database, config.limits.maxOutputBytes, config.limits.maxOutputsPerJob);
  }

  async start(): Promise<void> {
    await this.outputStore.initialize();
    for (const operation of this.database.listUnfinishedOperations()) {
      const uncertain = operation.status !== "pending";
      this.database.failOperation(operation.id, "gateway_restarted_during_operation",
        uncertain ? "gateway restarted after the control operation may have started; reconcile catalog before retrying" : "gateway restarted before the control operation started",
        uncertain ? "uncertain" : "failed");
    }
    const revision = this.database.catalogRevision();
    await Promise.all([...this.workers.values()].filter((worker) => worker.config.enabled).map(async (worker) => {
      try {
        await this.probeWorker(worker, this.config.catalog.refreshOnStart, revision);
      } catch { /* the status is retained; required readiness is reported by /readyz */ }
    }));
    if (this.primary.ready) {
      try { await this.refreshInventory(); }
      catch (error) { this.inventoryReady = false; this.primary.lastError = `catalog inventory: ${message(error)}`; }
    }
    await this.recoverJobs();
    await this.tick();
    const interval = Math.max(10, Math.min(250, this.config.timeouts.historyPollMs));
    this.timer = setInterval(() => { void this.tick(); }, interval);
    this.timer.unref();
    try { await this.outputStore.sweep(this.config.retention); }
    catch (error) { this.emit("warning", { message: `retention sweep failed: ${message(error)}` }); }
    this.retentionTimer = setInterval(() => {
      void this.outputStore.sweep(this.config.retention).catch((error: unknown) => this.emit("warning", { message: `retention sweep failed: ${message(error)}` }));
    }, this.config.retention.sweepIntervalMs);
    this.retentionTimer.unref();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    if (this.retentionTimer) clearInterval(this.retentionTimer);
    while (this.ticking) await sleep(5);
  }

  catalogRevision(): number { return this.database.catalogRevision(); }

  readyError(): string | undefined {
    for (const worker of this.workers.values()) {
      if (worker.config.enabled && worker.config.required && !worker.ready) return `${worker.config.id}: ${worker.lastError || worker.state}`;
    }
    return undefined;
  }

  workerSnapshots(): WorkerSnapshot[] {
    return [...this.workers.values()].map((worker) => ({
      id: worker.config.id, state: worker.state, ready: worker.ready, busy: worker.currentJobId !== "",
      required: worker.config.required, primary: worker.config.primary, currentJobId: worker.currentJobId,
      appliedRevision: worker.appliedRevision, secondsPerImage: worker.config.secondsPerImage,
      effectiveSecondsPerImage: this.effectiveSeconds(worker), ewmaSamples: worker.ewmaSamples,
      lastError: worker.lastError, deviceName: worker.deviceName,
    }));
  }

  getJob(id: string): JobRecord | undefined { return this.database.getJob(id); }
  queue(): { running: JobRecord[]; pending: JobRecord[] } {
    return {
      running: this.database.listJobs(["dispatching", "submitted", "running", "collecting", "uncertain"]),
      pending: this.database.listJobs(["queued"]),
    };
  }

  async enqueue(raw: unknown, idempotencyKey = ""): Promise<JobRecord> {
    const envelope = this.validateEnvelope(raw);
    const { profile } = profileAndCost(envelope as Record<string, unknown>);
    if (![...this.workers.values()].some((worker) => worker.config.enabled && worker.config.capabilities.includes(profile))) {
      throw new GatewayRequestError(503, "no_eligible_worker", `no worker supports profile ${profile}`);
    }
    await this.validateLoras(envelope.prompt);
    const id = randomUUID();
    let job: JobRecord;
    const clientId = typeof envelope.client_id === "string" ? envelope.client_id : "";
    if (idempotencyKey.length > 128) throw new GatewayRequestError(400, "invalid_idempotency_key", "X-Idempotency-Key must not exceed 128 characters");
    const requestJson = JSON.stringify(envelope);
    try {
      const inserted = this.database.insertJob(id, clientId, requestJson, this.config.limits.maxQueuedJobs,
        idempotencyKey ? { scope: clientId, key: idempotencyKey, requestHash: createHash("sha256").update(requestJson).digest("hex") } : undefined);
      job = inserted.job;
      if (!inserted.created) return job;
    }
    catch (error) {
      if (message(error) === "queue_full") throw new GatewayRequestError(503, "queue_full", "gateway queue is full");
      if (message(error) === "idempotency_conflict") throw new GatewayRequestError(409, "idempotency_conflict", "idempotency key was already used for a different prompt");
      throw error;
    }
    this.replanQueued();
    this.emit("job", { type: "status", jobId: id, status: "queued" });
    void this.tick();
    return this.database.getJob(id) ?? job;
  }

  private validateEnvelope(raw: unknown): PromptEnvelope {
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) throw new GatewayRequestError(400, "invalid_prompt", "request body must be an object");
    const envelope = raw as Record<string, unknown>;
    if (envelope.prompt === null || typeof envelope.prompt !== "object" || Array.isArray(envelope.prompt)) {
      throw new GatewayRequestError(400, "invalid_prompt", "request must contain a prompt object");
    }
    return raw as PromptEnvelope;
  }

  private async validateLoras(prompt: JsonObject): Promise<void> {
    const names: string[] = [];
    for (const node of Object.values(prompt)) {
      if (node === null || typeof node !== "object" || Array.isArray(node)) continue;
      const nodeObject = node as JsonObject;
      if (nodeObject.class_type !== "Lora Loader (LoraManager)") continue;
      const inputs = nodeObject.inputs;
      if (inputs === null || typeof inputs !== "object" || Array.isArray(inputs)) continue;
      const loras = (inputs as JsonObject).loras;
      if (loras === null || typeof loras !== "object" || Array.isArray(loras)) continue;
      const values = (loras as JsonObject).__value__;
      if (!Array.isArray(values)) continue;
      for (const rawLora of values) {
        if (rawLora === null || typeof rawLora !== "object" || Array.isArray(rawLora)) continue;
        const lora = rawLora as JsonObject;
        if (lora.active === false) continue;
        if (typeof lora.name === "string") names.push(lora.name);
      }
    }
    const normalizedNames: string[] = [];
    for (const name of names) {
      const normalized = name.replaceAll("\\", "/");
      if (name !== normalized || normalized.startsWith("/") || /^[A-Za-z]:/.test(normalized) || normalized.split("/").some((part) => part === ".." || part === "")) {
        throw new GatewayRequestError(400, "unsafe_lora_path", `LoRA path is not a normalized relative catalog key: ${name}`);
      }
      normalizedNames.push(normalized);
    }

    if (!normalizedNames.length) return;
    if (!this.inventoryReady) throw new GatewayRequestError(503, "catalog_unavailable", "LoRA catalog is not ready");

    let missing = normalizedNames.filter((name) => !this.inventory.has(name));
    if (missing.length > 0 && this.primary.ready) {
      try {
        await this.refreshInventoryOnce();
        missing = normalizedNames.filter((name) => !this.inventory.has(name));
      } catch (error) {
        this.emit("warning", { message: `LoRA inventory refresh during prompt validation failed: ${message(error)}` });
      }
    }
    if (missing.length > 0) {
      throw new GatewayRequestError(400, "unknown_lora", `LoRA is not active in the current catalog: ${missing[0]}`);
    }
  }

  private effectiveSeconds(worker: WorkerRuntime): number {
    return worker.ewmaSamples >= this.config.scheduler.ewmaMinSamples && worker.ewmaMs > 0 ? worker.ewmaMs / 1000 : worker.config.secondsPerImage;
  }

  private replanQueued(): void {
    if (this.dispatchPaused) return;
    const now = Date.now();
    const queued = this.database.listJobs(["queued"]);
    this.database.clearQueuedAssignments();
    const workers = [...this.workers.values()].map((worker) => {
      let availableInSeconds = 0;
      if (worker.currentJobId) {
        const current = this.database.getJob(worker.currentJobId);
        if (current?.submittedAtMs) {
          const elapsed = Math.max(0, now - current.submittedAtMs);
          availableInSeconds = elapsed < current.predictedDurationMs
            ? (current.predictedDurationMs - elapsed) / 1000
            : Math.max(this.config.timeouts.historyPollMs / 1000, elapsed / 1000 * 0.2);
        } else availableInSeconds = this.effectiveSeconds(worker);
      }
      return {
        id: worker.config.id,
        eligible: worker.config.enabled && worker.ready && !this.dispatchPaused,
        secondsPerImage: this.effectiveSeconds(worker), availableInSeconds,
        capabilities: new Set(worker.config.capabilities),
      };
    });
    const assignments = assignJobs(workers, queued.map((job) => {
      const envelope = JSON.parse(job.requestJson) as Record<string, unknown>;
      const profile = profileAndCost(envelope);
      return { id: job.id, ...profile };
    }), this.config.scheduler.tieEpsilonMs / 1000);
    for (const assignment of assignments) this.database.assignJob(assignment.jobId, assignment.workerId, assignment.predictedDurationMs);
  }

  private async tick(): Promise<void> {
    if (this.ticking || this.stopped) return;
    this.ticking = true;
    try {
      const now = Date.now();
      await Promise.all([...this.workers.values()].filter((worker) => worker.config.enabled && !worker.ready && !worker.currentJobId && worker.nextProbeAt <= now).map(async (worker) => {
        try { await this.probeWorker(worker, true, this.catalogRevision()); await this.refreshInventoryIfPrimary(worker); }
        catch { worker.nextProbeAt = Date.now() + this.config.catalog.retryMs; }
      }));
      await Promise.all([...this.workers.values()].filter((worker) => worker.currentJobId !== "").map((worker) => this.reconcileWorker(worker)));
      if (!this.dispatchPaused) {
        this.replanQueued();
        await Promise.all([...this.workers.values()].filter((worker) => worker.ready && !worker.currentJobId).map((worker) => this.dispatchWorker(worker)));
      }
    } finally { this.ticking = false; }
  }

  private async probeWorker(worker: WorkerRuntime, rebuild: boolean, revision: number): Promise<void> {
    worker.state = "syncing";
    try {
      worker.deviceName = await worker.client.validateDevice(worker.config.expectedDeviceName);
      if (rebuild) await worker.client.fullRebuild();
      worker.appliedRevision = revision;
      worker.ready = true;
      worker.state = worker.currentJobId ? "busy" : "ready";
      worker.lastError = "";
      worker.nextProbeAt = 0;
      this.database.setWorkerCatalog(worker.config.id, revision, "ready");
    } catch (error) {
      worker.ready = false;
      worker.state = "offline";
      worker.lastError = message(error);
      worker.nextProbeAt = Date.now() + this.config.catalog.retryMs;
      this.database.setWorkerCatalog(worker.config.id, worker.appliedRevision, "stale", worker.lastError);
      throw error;
    }
  }

  private async refreshInventoryIfPrimary(worker: WorkerRuntime): Promise<void> {
    if (worker === this.primary && worker.ready) {
      try { await this.refreshInventory(); }
      catch (error) { this.inventoryReady = false; worker.lastError = `catalog inventory: ${message(error)}`; }
    }
  }

  private async dispatchWorker(worker: WorkerRuntime): Promise<void> {
    const job = this.database.claimQueued(worker.config.id);
    if (!job) return;
    worker.currentJobId = job.id;
    worker.state = "busy";
    this.emit("job", { type: "execution_start", jobId: job.id, workerId: worker.config.id });
    try {
      const envelope = JSON.parse(job.requestJson) as PromptEnvelope;
      const response = await worker.client.submit(envelope, job.id);
      const backendId = typeof response.prompt_id === "string" ? response.prompt_id : job.id;
      if (backendId !== job.id) throw new Error(`worker did not preserve gateway prompt_id: ${backendId}`);
      this.database.markSubmitted(job.id, backendId, JSON.stringify(response));
    } catch (error) {
      if (error instanceof UpstreamHttpError) {
        const detail = new TextDecoder().decode(error.body).slice(0, 2000);
        this.database.markTerminal(job.id, "failed", JSON.stringify(errorHistory(job.id, `worker rejected prompt: ${detail}`)), "worker_rejected", `worker returned HTTP ${error.status}`);
        this.releaseWorker(worker);
      } else {
        await this.reconcileAmbiguousSubmission(worker, job.id, error);
      }
    }
  }

  private async reconcileAmbiguousSubmission(worker: WorkerRuntime, jobId: string, cause: unknown): Promise<void> {
    try {
      const history = await worker.client.history(jobId);
      if (history.found) {
        this.database.markSubmitted(jobId, jobId, "{}");
        if (history.terminal) await this.finishJob(worker, jobId, history.history, history.succeeded);
        return;
      }
      if (await worker.client.queueContains(jobId)) {
        this.database.markSubmitted(jobId, jobId, "{}");
        return;
      }
    } catch { /* preserve the original uncertainty */ }
    this.database.markUncertain(jobId, `submission outcome is unknown: ${message(cause)}`);
    worker.lastError = `job ${jobId} submission uncertain`;
  }

  private async reconcileWorker(worker: WorkerRuntime): Promise<void> {
    const job = this.database.getJob(worker.currentJobId);
    if (!job || ["succeeded", "failed", "cancelled"].includes(job.status)) { this.releaseWorker(worker); return; }
    try {
      const backendId = job.backendPromptId || job.id;
      const state = await worker.client.history(backendId);
      if (state.terminal) { await this.finishJob(worker, job.id, state.history, state.succeeded); return; }
      if (state.found || await worker.client.queueContains(backendId)) {
        if (job.status !== "uncertain") this.database.markRunning(job.id);
        return;
      }
      if (job.status === "dispatching") {
        this.database.markUncertain(job.id, "submission outcome is unknown after gateway restart");
        return;
      }
      if (job.submittedAtMs && Date.now() - job.submittedAtMs > this.config.timeouts.maxJobRuntimeMs) {
        this.database.markUncertain(job.id, "job exceeded maximum runtime and backend state is unknown");
      }
    } catch (error) {
      worker.ready = false;
      worker.state = "offline";
      worker.lastError = message(error);
      worker.nextProbeAt = Date.now() + this.config.catalog.retryMs;
    }
  }

  private async finishJob(worker: WorkerRuntime, jobId: string, history: JsonObject, succeeded: boolean): Promise<void> {
    const before = this.database.getJob(jobId);
    if (!succeeded) {
      const cancelled = before?.errorCode === "interrupt_requested";
      this.database.markTerminal(jobId, cancelled ? "cancelled" : "failed", JSON.stringify(history), cancelled ? "cancelled" : "worker_execution_failed", cancelled ? "job interrupted" : "worker execution failed");
      this.emit("job", { type: cancelled ? "execution_interrupted" : "execution_error", jobId, workerId: worker.config.id });
      this.releaseWorker(worker);
      return;
    }
    this.database.markCollecting(jobId);
    try {
      const collected = await this.outputStore.collect(jobId, worker.client, history);
      this.database.saveOutputs(jobId, JSON.stringify(collected.history), collected.outputs);
      const submittedAt = before?.submittedAtMs;
      if (submittedAt) {
        const stats = this.database.observeWorker(worker.config.id, Math.max(1, Date.now() - submittedAt), this.config.scheduler.ewmaAlpha);
        worker.ewmaMs = stats.ewmaMs;
        worker.ewmaSamples = stats.samples;
      }
      this.emit("job", { type: "executed", jobId, workerId: worker.config.id });
    } catch (error) {
      this.database.markUncertain(jobId, `backend succeeded but output collection failed: ${message(error)}`);
    }
    this.releaseWorker(worker);
  }

  private releaseWorker(worker: WorkerRuntime): void {
    worker.currentJobId = "";
    worker.state = worker.ready ? "ready" : "offline";
    this.replanQueued();
  }

  private async recoverJobs(): Promise<void> {
    for (const job of this.database.listJobs(["dispatching", "submitted", "running", "collecting", "uncertain"])) {
      const worker = this.workers.get(job.workerId);
      if (!worker) { this.database.markUncertain(job.id, `assigned worker ${job.workerId} is not configured`); continue; }
      if (worker.currentJobId && worker.currentJobId !== job.id) {
        this.database.markUncertain(job.id, `multiple recovered jobs claim worker ${job.workerId}`);
        continue;
      }
      worker.currentJobId = job.id;
      if (worker.ready) worker.state = "busy";
    }
    this.replanQueued();
  }

  async cancel(id: string): Promise<{ cancelled: boolean; state: string }> {
    const job = this.database.getJob(id);
    if (!job) throw new GatewayRequestError(404, "job_not_found", "gateway job was not found");
    if (job.status === "queued") {
      const cancelled = this.database.cancelQueued(id);
      this.replanQueued();
      return { cancelled, state: cancelled ? "cancelled" : this.database.getJob(id)?.status ?? job.status };
    }
    if (["submitted", "running", "uncertain"].includes(job.status)) {
      const worker = this.workers.get(job.workerId);
      if (!worker || worker.currentJobId !== id) throw new GatewayRequestError(409, "job_not_interruptible", "job has no active worker lease");
      await worker.client.interrupt(job.backendPromptId || id);
      this.database.markInterruptRequested(id);
      return { cancelled: false, state: "interrupt_requested" };
    }
    throw new GatewayRequestError(409, "job_not_cancellable", `job is ${job.status}`);
  }

  openOutput(filename: string, subfolder: string, type: string): { stream: ReadStream; contentType: string; size: number } | undefined {
    const resolved = this.outputStore.resolveOutput(filename, subfolder, type);
    return resolved ? { stream: createReadStream(resolved.path), contentType: resolved.record.contentType, size: resolved.record.sizeBytes } : undefined;
  }

  async systemStats(): Promise<JsonObject> { return this.primary.client.systemStats(); }

  async refreshInventory(): Promise<void> {
    const next = new Set<string>();
    const pageSize = this.config.catalog.pageSize;
    for (let page = 1; page <= 10000; page += 1) {
      const { value } = await this.primary.client.json(`/api/lm/loras/list?page=${page}&page_size=${pageSize}`);
      const items = value.items;
      if (!Array.isArray(items)) throw new Error("LoRA inventory response has no items array");
      for (const itemValue of items) {
        if (itemValue === null || typeof itemValue !== "object" || Array.isArray(itemValue)) continue;
        const item = itemValue as JsonObject;
        const key = catalogKey(item);
        if (key) next.add(key);
      }
      const totalPages = typeof value.total_pages === "number" ? value.total_pages : (items.length < pageSize ? page : page + 1);
      if (page >= totalPages || items.length === 0) break;
    }
    this.inventory = next;
    this.inventoryReady = true;
  }

  private async refreshInventoryOnce(): Promise<void> {
    if (!this.inventoryRefresh) {
      this.inventoryRefresh = this.refreshInventory().finally(() => {
        this.inventoryRefresh = undefined;
      });
    }
    await this.inventoryRefresh;
  }

  beginManagerOperation(scope: string, idempotencyKey: string, method: string, path: string, body: Uint8Array, requestTarget = path): { operation: CatalogOperation; created: boolean; conflict: boolean } {
    const hash = createHash("sha256").update(method).update("\n").update(requestTarget).update("\n").update(body).digest("hex");
    const kind = path === "/api/lm/download-model" ? "download" : path === "/api/lm/loras/scan" ? "reconcile" : "catalog_mutation";
    return this.database.beginOperation(scope, idempotencyKey, hash, method, path, kind);
  }

  getManagerOperation(id: string): CatalogOperation | undefined { return this.database.getOperation(id); }

  async runManagerOperation(operation: CatalogOperation, rawPathAndQuery: string, body: Uint8Array, contentType: string | undefined): Promise<ManagerResult> {
    let release!: () => void;
    const previous = this.managerSerial;
    this.managerSerial = new Promise<void>((resolvePromise) => { release = resolvePromise; });
    await previous;
    this.database.markOperationRunning(operation.id);
    const isDownload = operation.path === "/api/lm/download-model";
    const isScan = operation.path === "/api/lm/loras/scan";
    let upstream: Response | undefined;
    let responseBody: Uint8Array<ArrayBufferLike> = new Uint8Array();
    let responseContentType = "application/json";
    try {
      if (!isDownload) {
        this.dispatchPaused = true;
        await this.waitForDrain();
      }
      let upstreamPath = rawPathAndQuery;
      if (isScan) {
        const parsed = new URL(rawPathAndQuery, "http://gateway.invalid");
        parsed.searchParams.set("full_rebuild", "true");
        upstreamPath = `${parsed.pathname}${parsed.search}`;
      }
      upstream = await this.primary.client.proxy(upstreamPath, operation.method, body.byteLength > 0 ? body : undefined, contentType, isDownload);
      responseBody = await readResponseBytes(upstream, 16 * 1024 * 1024);
      responseContentType = upstream.headers.get("content-type") ?? "application/json";
      if (!upstream.ok) {
        this.database.completeOperation(operation.id, this.catalogRevision(), upstream.status, responseContentType, responseBody);
        return { operationId: operation.id, status: upstream.status, contentType: responseContentType, body: responseBody, revision: this.catalogRevision(), optionalUnavailable: [] };
      }
      if (isDownload) {
        this.dispatchPaused = true;
        await this.waitForDrain();
      }
      const revision = this.database.advanceCatalogRevision();
      for (const worker of this.workers.values()) {
        if (!worker.config.enabled) continue;
        worker.ready = false;
        worker.state = "stale";
        worker.lastError = `catalog revision ${revision} requires synchronization`;
        this.database.setWorkerCatalog(worker.config.id, worker.appliedRevision, "stale", worker.lastError);
      }
      const optionalUnavailable: string[] = [];
      for (const worker of this.workers.values()) {
        if (!worker.config.enabled) continue;
        try {
          if (!(isScan && worker === this.primary)) await this.probeWorker(worker, true, revision);
          else {
            worker.appliedRevision = revision; worker.ready = true; worker.state = "ready"; worker.lastError = "";
            this.database.setWorkerCatalog(worker.config.id, revision, "ready");
          }
        } catch (error) {
          if (worker.config.required) throw new Error(`required worker ${worker.config.id} catalog sync failed: ${message(error)}`);
          optionalUnavailable.push(worker.config.id);
        }
      }
      await this.refreshInventory();
      this.database.completeOperation(operation.id, revision, upstream.status, responseContentType, responseBody);
      return { operationId: operation.id, status: upstream.status, contentType: responseContentType, body: responseBody, revision, optionalUnavailable };
    } catch (error) {
      const code = upstream?.ok ? "catalog_barrier_failed" : "catalog_operation_failed";
      const status = upstream?.ok ? "uncertain" : "failed";
      this.database.failOperation(operation.id, code, message(error), status);
      throw new GatewayRequestError(503, code, message(error), operation.id);
    } finally {
      this.dispatchPaused = false;
      this.replanQueued();
      release();
      void this.tick();
    }
  }

  private async waitForDrain(): Promise<void> {
    const deadline = Date.now() + this.config.timeouts.catalogDrainMs;
    while (this.database.countInFlight() > 0) {
      if (Date.now() >= deadline) throw new Error(`catalog drain timed out with ${this.database.countInFlight()} in-flight job(s)`);
      await sleep(Math.min(100, this.config.timeouts.historyPollMs));
    }
  }
}

function catalogKey(item: JsonObject): string | undefined {
  for (const field of [item.relative_path, item.path, item.name]) {
    if (typeof field === "string" && field !== "") return field.replaceAll("\\", "/").replace(/^\/+/, "");
  }
  if (typeof item.file_path === "string" && item.file_path !== "") {
    const normalized = item.file_path.replaceAll("\\", "/");
    const marker = "/models/loras/";
    const markerIndex = normalized.toLowerCase().lastIndexOf(marker);
    if (markerIndex >= 0) return normalized.slice(markerIndex + marker.length);
    const basename = normalized.slice(normalized.lastIndexOf("/") + 1);
    if (typeof item.folder === "string" && item.folder !== "") return `${item.folder.replaceAll("\\", "/").replace(/^\/+|\/+$/g, "")}/${basename}`;
    return basename;
  }
  if (typeof item.file_name === "string" && item.file_name !== "") {
    const filename = item.file_name.endsWith(".safetensors") ? item.file_name : `${item.file_name}.safetensors`;
    return typeof item.folder === "string" && item.folder !== "" ? `${item.folder.replaceAll("\\", "/").replace(/^\/+|\/+$/g, "")}/${filename}` : filename;
  }
  return undefined;
}

export class GatewayRequestError extends Error {
  constructor(readonly statusCode: number, readonly code: string, detail: string, readonly operationId = "") { super(detail); }
}
