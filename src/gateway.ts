import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { ReadStream } from "node:fs";
import { createReadStream } from "node:fs";
import { createBatchAdapters } from "./batching/adapters.js";
import { BatchAdapterRegistry } from "./batching/registry.js";
import type { BatchAdapter, BatchCandidate, BatchPlan } from "./batching/types.js";
import type { GatewayDatabase } from "./database.js";
import { errorHistory } from "./database.js";
import { historyForMember, OutputStore } from "./output-store.js";
import { profileAndCost } from "./scheduler.js";
import type { ExecutionRecord, GatewayConfig, JsonObject, JobRecord, JobStatus, PromptEnvelope, WorkerConfig, WorkerSnapshot, WorkerState } from "./types.js";
import { readResponseBytes, UpstreamHttpError, WorkerClient } from "./worker-client.js";

interface WorkerRuntime {
  config: WorkerConfig;
  client: WorkerClient;
  state: WorkerState;
  ready: boolean;
  currentExecutionId: string;
  batchCapable: boolean;
  batchAdapters: Set<string>;
  appliedRevision: number;
  lastError: string;
  deviceName: string;
  ewmaMs: number;
  ewmaSamples: number;
  nextProbeAt: number;
  lastProbeAt: number;
  lastSystemStats: JsonObject;
}

export interface ManagerResult {
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
  readonly batchRegistry: BatchAdapterRegistry;
  private timer: NodeJS.Timeout | undefined;
  private retentionTimer: NodeJS.Timeout | undefined;
  private batchWakeTimer: NodeJS.Timeout | undefined;
  private batchWakeAt = 0;
  private ticking = false;
  private stopped = false;
  private dispatchPaused = false;
  private managerSerial: Promise<void> = Promise.resolve();
  private inventory = new Set<string>();
  private inventoryReady = false;
  private inventoryRefresh: Promise<void> | undefined;

  constructor(readonly config: GatewayConfig, readonly database: GatewayDatabase) {
    super();
    this.batchRegistry = new BatchAdapterRegistry(createBatchAdapters().filter((adapter) => config.batching.adapters?.[adapter.id] !== false));
    for (const workerConfig of config.workers) {
      const stats = database.workerStats(workerConfig.id);
      this.workers.set(workerConfig.id, {
        config: workerConfig,
        client: new WorkerClient(workerConfig.url, config.timeouts.workerRequestMs),
        state: workerConfig.enabled ? "offline" : "disabled", ready: false, currentExecutionId: "", batchCapable: false, batchAdapters: new Set(), appliedRevision: 0,
        lastError: "not probed", deviceName: "", ewmaMs: stats.ewmaMs, ewmaSamples: stats.samples, nextProbeAt: 0,
        lastProbeAt: 0, lastSystemStats: {},
      });
    }
    const primary = [...this.workers.values()].find((worker) => worker.config.enabled && worker.config.primary);
    if (!primary) throw new Error("primary worker missing");
    this.primary = primary;
    this.outputStore = new OutputStore(config.outputDirectory, database, config.limits.maxOutputBytes, config.limits.maxOutputsPerJob);
  }

  async start(): Promise<void> {
    await this.outputStore.initialize();
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
    if (this.batchWakeTimer) clearTimeout(this.batchWakeTimer);
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
      id: worker.config.id, state: worker.state, ready: worker.ready, busy: worker.currentExecutionId !== "",
      required: worker.config.required, primary: worker.config.primary,
      currentJobId: worker.currentExecutionId ? this.database.executionMembers(worker.currentExecutionId)[0]?.jobId ?? "" : "",
      currentExecutionId: worker.currentExecutionId,
      activeBatchSize: worker.currentExecutionId ? this.database.getExecution(worker.currentExecutionId)?.batchSize ?? 0 : 0,
      batchCapable: worker.batchCapable,
      maxBatchSize: worker.batchCapable ? this.maximumConfiguredBatch(worker) : 1,
      batchStrategies: [...worker.batchAdapters].sort(),
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

  private configuredBatchSize(worker: WorkerRuntime, profile: string, adapter: BatchAdapter): number {
    if (!this.config.batching.enabled || !worker.batchAdapters.has(adapter.id)) return 1;
    const configured = this.config.batching.workers[worker.config.id]?.[profile] ?? 1;
    return Math.min(configured, adapter.maxBatchSize);
  }

  private maximumConfiguredBatch(worker: WorkerRuntime): number {
    return Math.max(1, ...Object.values(this.config.batching.workers[worker.config.id] ?? {}));
  }

  private scheduleBatchWake(atMs: number): void {
    if (this.stopped || (this.batchWakeTimer && this.batchWakeAt <= atMs)) return;
    if (this.batchWakeTimer) clearTimeout(this.batchWakeTimer);
    this.batchWakeAt = atMs;
    this.batchWakeTimer = setTimeout(() => {
      this.batchWakeTimer = undefined;
      this.batchWakeAt = 0;
      void this.tick();
    }, Math.max(0, atMs - Date.now()));
    this.batchWakeTimer.unref();
  }

  private async tick(): Promise<void> {
    if (this.ticking || this.stopped) return;
    this.ticking = true;
    try {
      const now = Date.now();
      await Promise.all([...this.workers.values()].filter((worker) => worker.config.enabled && !worker.ready && !worker.currentExecutionId && worker.nextProbeAt <= now).map(async (worker) => {
        try { await this.probeWorker(worker, true, this.catalogRevision()); await this.refreshInventoryIfPrimary(worker); }
        catch { worker.nextProbeAt = Date.now() + this.config.catalog.retryMs; }
      }));
      await Promise.all([...this.workers.values()].filter((worker) => worker.currentExecutionId !== "").map((worker) => this.reconcileWorker(worker)));
      if (!this.dispatchPaused) {
        const idleWorkers = [...this.workers.values()]
          .filter((worker) => worker.ready && !worker.currentExecutionId)
          .sort((left, right) => this.effectiveSeconds(left) - this.effectiveSeconds(right));
        await Promise.all(idleWorkers.map((worker) => this.dispatchWorker(worker)));
      }
    } finally { this.ticking = false; }
  }

  private async probeWorker(worker: WorkerRuntime, rebuild: boolean, revision: number): Promise<void> {
    worker.state = "syncing";
    try {
      const device = await worker.client.probeDevice(worker.config.expectedDeviceName);
      worker.deviceName = device.deviceName;
      worker.lastSystemStats = device.systemStats;
      worker.lastProbeAt = Date.now();
      if (rebuild) await worker.client.fullRebuild();
      worker.batchAdapters.clear();
      worker.batchCapable = false;
      if (this.config.batching.enabled && this.maximumConfiguredBatch(worker) > 1) {
        const requiredNodes = [...new Set(this.batchRegistry.adapters.flatMap((adapter) => adapter.requiredBackendNodes))];
        const availableNodes = new Set<string>();
        const support = await Promise.all(requiredNodes.map(async (nodeName) => {
          try {
            return { nodeName, supported: await worker.client.supportsNode(nodeName) };
          } catch (error) {
            this.emit("warning", { message: `batch capability probe failed for ${worker.config.id}/${nodeName}: ${message(error)}` });
            return { nodeName, supported: false };
          }
        }));
        for (const result of support) if (result.supported) availableNodes.add(result.nodeName);
        for (const adapter of this.batchRegistry.adapters) {
          if (this.batchRegistry.compatible(adapter, availableNodes)) worker.batchAdapters.add(adapter.id);
        }
        worker.batchCapable = worker.batchAdapters.size > 0;
      }
      worker.appliedRevision = revision;
      worker.ready = true;
      worker.state = worker.currentExecutionId ? "busy" : "ready";
      worker.lastError = "";
      worker.nextProbeAt = 0;
      this.database.setWorkerCatalog(worker.config.id, revision, "ready");
    } catch (error) {
      worker.ready = false;
      worker.batchCapable = false;
      worker.batchAdapters.clear();
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
    const queued = this.database.listJobs(["queued"]);
    let execution: ExecutionRecord | undefined;
    let memberJobs: JobRecord[] = [];
    for (let candidateIndex = 0; candidateIndex < queued.length; candidateIndex += 1) {
      const candidate = queued[candidateIndex]!;
      const envelope = JSON.parse(candidate.requestJson) as PromptEnvelope;
      const { profile, costFactor } = profileAndCost(envelope as Record<string, unknown>);
      if (!worker.config.capabilities.includes(profile)) continue;
      const analyzed = this.batchRegistry.assess(envelope);
      const adapter = analyzed.candidate ? this.batchRegistry.get(analyzed.candidate.adapterId) : undefined;
      const maxBatchSize = adapter ? this.configuredBatchSize(worker, profile, adapter) : 1;
      let selected: Array<{ job: JobRecord; batch: BatchCandidate }> = [];
      if (adapter && maxBatchSize > 1 && worker.batchAdapters.has(adapter.id)
        && !(candidate.batchBlocked && (!candidate.batchBlockedAdapter || candidate.batchBlockedAdapter === adapter.id))) {
        const firstBatch = analyzed.candidate!;
        for (const possible of queued.slice(candidateIndex)) {
          if (possible.batchBlocked && (!possible.batchBlockedAdapter || possible.batchBlockedAdapter === adapter.id)) continue;
          const possibleEnvelope = JSON.parse(possible.requestJson) as PromptEnvelope;
          const possibleAssessment = this.batchRegistry.assess(possibleEnvelope);
          const possibleBatch = possibleAssessment.candidate;
          if (possibleBatch?.adapterId === firstBatch.adapterId && possibleBatch.mergeKey === firstBatch.mergeKey) {
            selected.push({ job: possible, batch: possibleBatch });
          }
          if (selected.length >= maxBatchSize) break;
        }
        if (selected.length === 1 && Date.now() < candidate.createdAtMs + this.config.batching.mergeWindowMs) {
          this.scheduleBatchWake(candidate.createdAtMs + this.config.batching.mergeWindowMs);
          continue;
        }
      }

      let executionId = candidate.id;
      let executionEnvelope = envelope;
      let strategyId = "";
      let strategyVersion = 0;
      let batchPlanJson = "";
      memberJobs = [candidate];
      if (selected.length >= 2) {
        const batchExecutionId = randomUUID();
        try {
          const physical = adapter!.merge(selected.map((item) => item.batch), batchExecutionId);
          executionEnvelope = physical.envelope;
          executionId = batchExecutionId;
          strategyId = physical.plan.adapter_id;
          strategyVersion = physical.plan.adapter_version;
          batchPlanJson = JSON.stringify(physical.plan);
          memberJobs = selected.map((item) => item.job);
        } catch (error) {
          this.emit("warning", { message: `batch graph transform rejected queued group: ${message(error)}` });
        }
      }
      execution = this.database.claimQueuedGroup(
        memberJobs.map((job) => job.id), executionId, worker.config.id,
        JSON.stringify(executionEnvelope),
        Math.round(this.effectiveSeconds(worker) * costFactor * 1000 * memberJobs.length),
        strategyId, strategyVersion, batchPlanJson,
      );
      if (execution) break;
    }
    if (!execution) return;
    worker.currentExecutionId = execution.id;
    worker.state = "busy";
    for (const job of memberJobs) this.emit("job", { type: "execution_start", jobId: job.id, executionId: execution.id, workerId: worker.config.id });
    try {
      const executionEnvelope = JSON.parse(execution.requestJson) as PromptEnvelope;
      const response = await worker.client.submit(executionEnvelope, execution.id);
      const backendId = typeof response.prompt_id === "string" ? response.prompt_id : execution.id;
      if (backendId !== execution.id) throw new Error(`worker did not preserve gateway execution prompt_id: ${backendId}`);
      this.database.markExecutionSubmitted(execution.id, backendId, JSON.stringify(response));
    } catch (error) {
      if (error instanceof UpstreamHttpError) {
        const detail = new TextDecoder().decode(error.body).slice(0, 2000);
        if (execution.batchSize > 1) {
          this.database.requeueBatchExecution(execution.id, `worker returned HTTP ${error.status}: ${detail}`);
          for (const job of memberJobs) this.emit("job", { type: "batch_fallback", jobId: job.id, executionId: execution.id, workerId: worker.config.id });
        } else {
          this.failExecutionMembers(execution, `worker rejected prompt: ${detail}`, "worker_rejected");
        }
        this.releaseWorker(worker);
        void this.tick();
      } else {
        await this.reconcileAmbiguousSubmission(worker, execution.id, error);
      }
    }
  }

  private async reconcileAmbiguousSubmission(worker: WorkerRuntime, executionId: string, cause: unknown): Promise<void> {
    try {
      const history = await worker.client.history(executionId);
      if (history.found) {
        this.database.markExecutionSubmitted(executionId, executionId, "{}");
        if (history.terminal) await this.finishExecution(worker, executionId, history.history, history.succeeded);
        return;
      }
      if (await worker.client.queueContains(executionId)) {
        this.database.markExecutionSubmitted(executionId, executionId, "{}");
        return;
      }
    } catch { /* preserve the original uncertainty */ }
    this.database.markExecutionUncertain(executionId, `submission outcome is unknown: ${message(cause)}`);
    worker.lastError = `execution ${executionId} submission uncertain`;
  }

  private async reconcileWorker(worker: WorkerRuntime): Promise<void> {
    const execution = this.database.getExecution(worker.currentExecutionId);
    if (!execution || ["succeeded", "failed", "cancelled"].includes(execution.status)) { this.releaseWorker(worker); return; }
    try {
      const backendId = execution.backendPromptId || execution.id;
      const state = await worker.client.history(backendId);
      if (state.terminal) { await this.finishExecution(worker, execution.id, state.history, state.succeeded); return; }
      if (state.found || await worker.client.queueContains(backendId)) {
        if (execution.status !== "uncertain") this.database.markExecutionRunning(execution.id);
        return;
      }
      if (execution.status === "dispatching") {
        this.database.markExecutionUncertain(execution.id, "submission outcome is unknown after gateway restart");
        return;
      }
      if (execution.submittedAtMs && Date.now() - execution.submittedAtMs > this.config.timeouts.maxJobRuntimeMs) {
        this.failExecutionMembers(execution, "execution exceeded maximum runtime and no longer exists in backend history or queue", "backend_state_lost");
        this.releaseWorker(worker);
      }
    } catch (error) {
      worker.ready = false;
      worker.state = "offline";
      worker.lastError = message(error);
      worker.nextProbeAt = Date.now() + this.config.catalog.retryMs;
    }
  }

  private async finishExecution(worker: WorkerRuntime, executionId: string, history: JsonObject, succeeded: boolean): Promise<void> {
    const before = this.database.getExecution(executionId);
    if (!before) { this.releaseWorker(worker); return; }
    const activeMembers = this.database.nonTerminalExecutionMembers(executionId);
    if (activeMembers.length === 0) {
      this.database.markExecutionTerminal(executionId, "cancelled", "cancelled", "all execution members were cancelled");
      this.releaseWorker(worker);
      return;
    }
    if (!succeeded) {
      if (before.batchSize > 1 && before.errorCode !== "interrupt_requested") {
        this.database.requeueBatchExecution(executionId, "worker batch execution failed; retrying members individually");
        for (const member of activeMembers) this.emit("job", { type: "batch_fallback", jobId: member.jobId, executionId, workerId: worker.config.id });
      } else {
        const cancelled = before.errorCode === "interrupt_requested";
        this.failExecutionMembers(before, cancelled ? "job interrupted" : "worker execution failed", cancelled ? "cancelled" : "worker_execution_failed", history, cancelled);
      }
      this.releaseWorker(worker);
      void this.tick();
      return;
    }
    this.database.markExecutionCollecting(executionId);
    try {
      const batchBinding = this.batchBinding(before);
      const collected = await this.outputStore.collectExecution(
        executionId, activeMembers, worker.client, history, batchBinding?.adapter, batchBinding?.plan,
      );
      this.database.saveExecutionOutputs(executionId, collected.map((member) => ({
        jobId: member.jobId, historyJson: JSON.stringify(member.history), outputs: member.outputs,
      })));
      const submittedAt = before.submittedAtMs;
      if (submittedAt) {
        const elapsedPerImage = Math.max(1, Math.round((Date.now() - submittedAt) / before.batchSize));
        const stats = this.database.observeWorker(worker.config.id, elapsedPerImage, this.config.scheduler.ewmaAlpha);
        worker.ewmaMs = stats.ewmaMs;
        worker.ewmaSamples = stats.samples;
      }
      for (const member of activeMembers) this.emit("job", { type: "executed", jobId: member.jobId, executionId, workerId: worker.config.id });
      this.releaseWorker(worker);
    } catch (error) {
      this.database.markExecutionUncertain(executionId, `backend succeeded but output collection failed: ${message(error)}`, "output_collection_failed");
      worker.lastError = `execution ${executionId} output collection will be retried: ${message(error)}`;
    }
  }

  private failExecutionMembers(
    execution: ExecutionRecord, detail: string, errorCode: string,
    backendHistory?: JsonObject, cancelled = false,
  ): void {
    for (const member of this.database.nonTerminalExecutionMembers(execution.id)) {
      let history: JsonObject = errorHistory(member.jobId, detail) as JsonObject;
      if (backendHistory) {
        try { history = historyForMember(backendHistory, execution.backendPromptId || execution.id, member.jobId); }
        catch { /* retain a gateway-generated terminal history */ }
      }
      this.database.markTerminal(member.jobId, cancelled ? "cancelled" : "failed", JSON.stringify(history), errorCode, detail);
      this.emit("job", { type: cancelled ? "execution_interrupted" : "execution_error", jobId: member.jobId, executionId: execution.id, workerId: execution.workerId });
    }
    this.database.markExecutionTerminal(execution.id, cancelled ? "cancelled" : "failed", errorCode, detail);
  }

  private batchBinding(execution: ExecutionRecord): { adapter: BatchAdapter; plan: BatchPlan } | undefined {
    if (!execution.strategyId || !execution.batchPlanJson) return undefined;
    const adapter = this.batchRegistry.get(execution.strategyId);
    if (!adapter) throw new Error(`batch adapter ${execution.strategyId} is unavailable`);
    if (adapter.version !== execution.strategyVersion) {
      throw new Error(`batch adapter ${execution.strategyId} version ${execution.strategyVersion} is unavailable`);
    }
    let plan: BatchPlan;
    try { plan = JSON.parse(execution.batchPlanJson) as BatchPlan; }
    catch { throw new Error(`batch execution ${execution.id} has invalid persisted plan`); }
    if (plan.adapter_id !== adapter.id || plan.adapter_version !== adapter.version) {
      throw new Error(`batch execution ${execution.id} plan does not match adapter ${adapter.id}`);
    }
    return { adapter, plan };
  }

  private releaseWorker(worker: WorkerRuntime): void {
    worker.currentExecutionId = "";
    worker.state = worker.ready ? "ready" : "offline";
  }

  private async recoverJobs(): Promise<void> {
    for (const execution of this.database.listExecutions(["dispatching", "submitted", "running", "collecting", "uncertain"])) {
      const worker = this.workers.get(execution.workerId);
      if (!worker) { this.database.markExecutionUncertain(execution.id, `assigned worker ${execution.workerId} is not configured`); continue; }
      if (worker.currentExecutionId && worker.currentExecutionId !== execution.id) {
        this.database.markExecutionUncertain(execution.id, `multiple recovered executions claim worker ${execution.workerId}`);
        continue;
      }
      worker.currentExecutionId = execution.id;
      if (worker.ready) worker.state = "busy";
    }
  }

  async cancel(id: string): Promise<{ cancelled: boolean; state: string }> {
    const job = this.database.getJob(id);
    if (!job) throw new GatewayRequestError(404, "job_not_found", "gateway job was not found");
    if (job.status === "queued") {
      const cancelled = this.database.cancelQueued(id);
      return { cancelled, state: cancelled ? "cancelled" : this.database.getJob(id)?.status ?? job.status };
    }
    if (["dispatching", "submitted", "running", "uncertain"].includes(job.status)) {
      const execution = this.database.activeExecutionForJob(id);
      if (!execution) throw new GatewayRequestError(409, "job_not_interruptible", "job has no active physical execution");
      const worker = this.workers.get(job.workerId);
      if (!worker || worker.currentExecutionId !== execution.id) throw new GatewayRequestError(409, "job_not_interruptible", "job has no active worker lease");
      if (execution.batchSize > 1) {
        const cancelled = this.database.cancelExecutionMember(id);
        if (cancelled && this.database.nonTerminalExecutionMembers(execution.id).length === 0) {
          try { await worker.client.interrupt(execution.backendPromptId || execution.id); }
          catch (error) { worker.lastError = `final batch-member interrupt failed: ${message(error)}`; }
          this.database.markExecutionUncertain(execution.id, "all batch members cancelled; awaiting backend terminal state", "interrupt_requested");
        }
        return { cancelled, state: cancelled ? "cancelled" : this.database.getJob(id)?.status ?? job.status };
      }
      await worker.client.interrupt(execution.backendPromptId || execution.id);
      this.database.markExecutionUncertain(execution.id, "interrupt requested; awaiting backend terminal state", "interrupt_requested");
      return { cancelled: false, state: "interrupt_requested" };
    }
    throw new GatewayRequestError(409, "job_not_cancellable", `job is ${job.status}`);
  }

  openOutput(filename: string, subfolder: string, type: string): { stream: ReadStream; contentType: string; size: number } | undefined {
    const resolved = this.outputStore.resolveOutput(filename, subfolder, type);
    return resolved ? { stream: createReadStream(resolved.path), contentType: resolved.record.contentType, size: resolved.record.sizeBytes } : undefined;
  }

  async systemStats(): Promise<JsonObject> { return this.primary.client.systemStats(); }

  clusterStatus(): Record<string, unknown> {
    const queue = this.queue();
    const groupCounts = new Map<string, number>();
    const rejectionCounts = new Map<string, number>();
    for (const job of queue.pending) {
      const assessment = this.batchRegistry.assess(JSON.parse(job.requestJson) as PromptEnvelope);
      const candidate = assessment.candidate;
      if (!candidate) {
        for (const rejection of assessment.rejections) {
          if (rejection.kind !== "reject") continue;
          rejectionCounts.set(rejection.reason, (rejectionCounts.get(rejection.reason) ?? 0) + 1);
        }
        continue;
      }
      if (job.batchBlocked && (!job.batchBlockedAdapter || job.batchBlockedAdapter === candidate.adapterId)) continue;
      const groupKey = `${candidate.adapterId}:${candidate.mergeKey}`;
      groupCounts.set(groupKey, (groupCounts.get(groupKey) ?? 0) + 1);
    }
    const activeExecutions = this.database.listExecutions(["dispatching", "submitted", "running", "collecting", "uncertain"]);
    return {
      generated_at_ms: Date.now(),
      catalog_revision: this.catalogRevision(),
      queue: { pending: queue.pending.length, running_members: queue.running.length, active_executions: activeExecutions.length },
      batching: {
        enabled: this.config.batching.enabled,
        merge_window_ms: this.config.batching.mergeWindowMs,
        queued_candidates: [...groupCounts.values()].reduce((total, count) => total + count, 0),
        mergeable_groups: [...groupCounts.values()].filter((count) => count >= 2).length,
        rejections: Object.fromEntries(rejectionCounts),
        active_executions: activeExecutions.map((execution) => ({
          id: execution.id, worker_id: execution.workerId, status: execution.status,
          batch_size: execution.batchSize,
          member_job_ids: this.database.executionMembers(execution.id).map((member) => member.jobId),
          submitted_at_ms: execution.submittedAtMs,
        })),
      },
      workers: this.workerSnapshots().map((snapshot) => {
        const runtime = this.workers.get(snapshot.id)!;
        return { ...snapshot, last_probe_at_ms: runtime.lastProbeAt, system_stats: runtime.lastSystemStats };
      }),
    };
  }

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

  async runCatalogMutation(rawPathAndQuery: string, method: string, body: Uint8Array, contentType: string | undefined): Promise<ManagerResult> {
    let release!: () => void;
    const previous = this.managerSerial;
    this.managerSerial = new Promise<void>((resolvePromise) => { release = resolvePromise; });
    await previous;
    const parsedRequest = new URL(rawPathAndQuery, "http://gateway.invalid");
    const isScan = parsedRequest.pathname === "/api/lm/loras/scan";
    let upstream: Response | undefined;
    let responseBody: Uint8Array<ArrayBufferLike> = new Uint8Array();
    let responseContentType = "application/json";
    try {
      this.dispatchPaused = true;
      await this.waitForDrain();
      let upstreamPath = rawPathAndQuery;
      if (isScan) {
        parsedRequest.searchParams.set("full_rebuild", "true");
        upstreamPath = `${parsedRequest.pathname}${parsedRequest.search}`;
      }
      upstream = await this.primary.client.proxy(upstreamPath, method, body.byteLength > 0 ? body : undefined, contentType, false);
      responseBody = await readResponseBytes(upstream, 16 * 1024 * 1024);
      responseContentType = upstream.headers.get("content-type") ?? "application/json";
      if (!upstream.ok) {
        return { status: upstream.status, contentType: responseContentType, body: responseBody, revision: this.catalogRevision(), optionalUnavailable: [] };
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
      return { status: upstream.status, contentType: responseContentType, body: responseBody, revision, optionalUnavailable };
    } catch (error) {
      const code = upstream?.ok ? "catalog_barrier_failed" : "catalog_operation_failed";
      throw new GatewayRequestError(503, code, message(error));
    } finally {
      this.dispatchPaused = false;
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
