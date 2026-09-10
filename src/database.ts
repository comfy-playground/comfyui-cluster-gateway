import { randomUUID } from "node:crypto";
import { DatabaseSync, type SQLOutputValue } from "node:sqlite";
import type { ExecutionMember, ExecutionRecord, ExecutionStatus, JobRecord, JobStatus, OutputRecord } from "./types.js";

type Row = Record<string, SQLOutputValue>;

const ACTIVE = ["queued", "dispatching", "submitted", "running", "collecting", "uncertain"] as const;
const IN_FLIGHT = ["dispatching", "submitted", "running", "collecting", "uncertain"] as const;

function num(value: SQLOutputValue | undefined): number { return Number(value ?? 0); }
function str(value: SQLOutputValue | undefined): string { return typeof value === "string" ? value : ""; }

function jobFrom(row: Row): JobRecord {
  return {
    number: num(row.number), id: str(row.id), clientId: str(row.client_id), requestJson: str(row.request_json),
    status: str(row.status) as JobStatus, workerId: str(row.worker_id), backendPromptId: str(row.backend_prompt_id),
    backendResponseJson: str(row.backend_response_json), historyJson: str(row.history_json),
    predictedDurationMs: num(row.predicted_duration_ms), errorCode: str(row.error_code), errorMessage: str(row.error_message),
    createdAtMs: num(row.created_at_ms), updatedAtMs: num(row.updated_at_ms),
    submittedAtMs: row.submitted_at_ms === null ? null : num(row.submitted_at_ms),
    completedAtMs: row.completed_at_ms === null ? null : num(row.completed_at_ms),
    batchBlocked: num(row.batch_blocked) !== 0,
    batchBlockedAdapter: str(row.batch_blocked_adapter),
  };
}

function executionFrom(row: Row): ExecutionRecord {
  return {
    id: str(row.id), workerId: str(row.worker_id), requestJson: str(row.request_json),
    status: str(row.status) as ExecutionStatus, backendPromptId: str(row.backend_prompt_id),
    backendResponseJson: str(row.backend_response_json), predictedDurationMs: num(row.predicted_duration_ms),
    batchSize: num(row.batch_size), errorCode: str(row.error_code), errorMessage: str(row.error_message),
    strategyId: str(row.strategy_id), strategyVersion: num(row.strategy_version), batchPlanJson: str(row.batch_plan_json),
    createdAtMs: num(row.created_at_ms), updatedAtMs: num(row.updated_at_ms),
    submittedAtMs: row.submitted_at_ms === null ? null : num(row.submitted_at_ms),
    completedAtMs: row.completed_at_ms === null ? null : num(row.completed_at_ms),
  };
}

function executionMemberFrom(row: Row): ExecutionMember {
  return { executionId: str(row.execution_id), jobId: str(row.job_id), memberIndex: num(row.member_index) };
}

export class GatewayDatabase {
  readonly db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path, { allowExtension: false });
    this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;");
    this.migrate();
  }

  close(): void { this.db.close(); }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        number INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        client_id TEXT NOT NULL DEFAULT '',
        request_json TEXT NOT NULL,
        status TEXT NOT NULL,
        worker_id TEXT NOT NULL DEFAULT '',
        backend_prompt_id TEXT NOT NULL DEFAULT '',
        backend_response_json TEXT NOT NULL DEFAULT '',
        history_json TEXT NOT NULL DEFAULT '',
        predicted_duration_ms INTEGER NOT NULL DEFAULT 0,
        error_code TEXT NOT NULL DEFAULT '',
        error_message TEXT NOT NULL DEFAULT '',
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        submitted_at_ms INTEGER,
        completed_at_ms INTEGER,
        batch_blocked INTEGER NOT NULL DEFAULT 0,
        batch_blocked_adapter TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS jobs_status_number ON jobs(status, number);
      CREATE INDEX IF NOT EXISTS jobs_worker_status ON jobs(worker_id, status);
      CREATE TABLE IF NOT EXISTS outputs (
        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        node_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        filename TEXT NOT NULL,
        subfolder TEXT NOT NULL,
        type TEXT NOT NULL,
        relative_path TEXT NOT NULL UNIQUE,
        content_type TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        PRIMARY KEY(job_id, node_id, ordinal)
      );
      CREATE TABLE IF NOT EXISTS worker_stats (
        worker_id TEXT PRIMARY KEY,
        ewma_ms REAL NOT NULL DEFAULT 0,
        samples INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS worker_model_stats (
        worker_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        ewma_ms REAL NOT NULL DEFAULT 0,
        samples INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(worker_id, model_id)
      );
      CREATE TABLE IF NOT EXISTS catalog_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        current_revision INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
      INSERT OR IGNORE INTO catalog_state(singleton,current_revision,updated_at_ms) VALUES(1,0,0);
      CREATE TABLE IF NOT EXISTS worker_catalog (
        worker_id TEXT PRIMARY KEY,
        applied_revision INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'stale',
        last_error TEXT NOT NULL DEFAULT '',
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS generation_idempotency (
        scope TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
        created_at_ms INTEGER NOT NULL,
        PRIMARY KEY(scope,idempotency_key)
      );
      CREATE TABLE IF NOT EXISTS executions (
        id TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        request_json TEXT NOT NULL,
        status TEXT NOT NULL,
        backend_prompt_id TEXT NOT NULL DEFAULT '',
        backend_response_json TEXT NOT NULL DEFAULT '',
        predicted_duration_ms INTEGER NOT NULL DEFAULT 0,
        batch_size INTEGER NOT NULL,
        error_code TEXT NOT NULL DEFAULT '',
        error_message TEXT NOT NULL DEFAULT '',
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        submitted_at_ms INTEGER,
        completed_at_ms INTEGER,
        strategy_id TEXT NOT NULL DEFAULT '',
        strategy_version INTEGER NOT NULL DEFAULT 0,
        batch_plan_json TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS executions_worker_status ON executions(worker_id,status);
      CREATE TABLE IF NOT EXISTS execution_members (
        execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        member_index INTEGER NOT NULL,
        PRIMARY KEY(execution_id,job_id),
        UNIQUE(execution_id,member_index)
      );
      CREATE INDEX IF NOT EXISTS execution_members_job ON execution_members(job_id);
    `);
    const jobColumns = this.db.prepare("PRAGMA table_info(jobs)").all().map((row) => str(row.name));
    if (!jobColumns.includes("batch_blocked")) this.db.exec("ALTER TABLE jobs ADD COLUMN batch_blocked INTEGER NOT NULL DEFAULT 0");
    if (!jobColumns.includes("batch_blocked_adapter")) this.db.exec("ALTER TABLE jobs ADD COLUMN batch_blocked_adapter TEXT NOT NULL DEFAULT ''");
    const executionColumns = this.db.prepare("PRAGMA table_info(executions)").all().map((row) => str(row.name));
    if (!executionColumns.includes("strategy_id")) this.db.exec("ALTER TABLE executions ADD COLUMN strategy_id TEXT NOT NULL DEFAULT ''");
    if (!executionColumns.includes("strategy_version")) this.db.exec("ALTER TABLE executions ADD COLUMN strategy_version INTEGER NOT NULL DEFAULT 0");
    if (!executionColumns.includes("batch_plan_json")) this.db.exec("ALTER TABLE executions ADD COLUMN batch_plan_json TEXT NOT NULL DEFAULT ''");
    this.adoptLegacyExecutions();
  }

  private adoptLegacyExecutions(): void {
    const placeholders = IN_FLIGHT.map(() => "?").join(",");
    const jobs = this.db.prepare(`SELECT * FROM jobs WHERE status IN (${placeholders}) AND NOT EXISTS (
      SELECT 1 FROM execution_members WHERE execution_members.job_id=jobs.id
    )`).all(...IN_FLIGHT).map(jobFrom);
    for (const job of jobs) {
      const executionId = job.backendPromptId || job.id;
      this.db.prepare(`INSERT OR IGNORE INTO executions(
        id,worker_id,request_json,status,backend_prompt_id,backend_response_json,predicted_duration_ms,batch_size,
        error_code,error_message,created_at_ms,updated_at_ms,submitted_at_ms,completed_at_ms,strategy_id,strategy_version,batch_plan_json
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
        executionId, job.workerId, job.requestJson, job.status, job.backendPromptId,
        job.backendResponseJson, job.predictedDurationMs, 1, job.errorCode, job.errorMessage,
        job.createdAtMs, job.updatedAtMs, job.submittedAtMs, job.completedAtMs, "", 0, "",
      );
      this.db.prepare("INSERT OR IGNORE INTO execution_members(execution_id,job_id,member_index) VALUES(?,?,0)")
        .run(executionId, job.id);
    }
  }

  transaction<T>(work: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  insertJob(id: string, clientId: string, requestJson: string, maxActive: number,
    idempotency?: { scope: string; key: string; requestHash: string }): { job: JobRecord; created: boolean } {
    return this.transaction(() => {
      if (idempotency) {
        const existing = this.db.prepare("SELECT request_hash,job_id FROM generation_idempotency WHERE scope=? AND idempotency_key=?").get(idempotency.scope, idempotency.key);
        if (existing) {
          if (str(existing.request_hash) !== idempotency.requestHash) throw new Error("idempotency_conflict");
          const existingJob = this.getJob(str(existing.job_id));
          if (!existingJob) throw new Error("idempotency record references a missing job");
          return { job: existingJob, created: false };
        }
      }
      const placeholders = ACTIVE.map(() => "?").join(",");
      const count = this.db.prepare(`SELECT COUNT(*) AS count FROM jobs WHERE status IN (${placeholders})`).get(...ACTIVE);
      if (num(count?.count) >= maxActive) throw new Error("queue_full");
      const now = Date.now();
      this.db.prepare("INSERT INTO jobs(id,client_id,request_json,status,created_at_ms,updated_at_ms) VALUES(?,?,?,'queued',?,?)")
        .run(id, clientId, requestJson, now, now);
      if (idempotency) {
        this.db.prepare("INSERT INTO generation_idempotency(scope,idempotency_key,request_hash,job_id,created_at_ms) VALUES(?,?,?,?,?)")
          .run(idempotency.scope, idempotency.key, idempotency.requestHash, id, now);
      }
      const job = this.getJob(id);
      if (!job) throw new Error("inserted job disappeared");
      return { job, created: true };
    });
  }

  getJob(id: string): JobRecord | undefined {
    const row = this.db.prepare("SELECT * FROM jobs WHERE id=?").get(id);
    return row ? jobFrom(row) : undefined;
  }

  listJobs(statuses?: readonly JobStatus[]): JobRecord[] {
    if (!statuses || statuses.length === 0) return this.db.prepare("SELECT * FROM jobs ORDER BY number").all().map(jobFrom);
    const placeholders = statuses.map(() => "?").join(",");
    return this.db.prepare(`SELECT * FROM jobs WHERE status IN (${placeholders}) ORDER BY number`).all(...statuses).map(jobFrom);
  }

  getExecution(id: string): ExecutionRecord | undefined {
    const row = this.db.prepare("SELECT * FROM executions WHERE id=?").get(id);
    return row ? executionFrom(row) : undefined;
  }

  listExecutions(statuses?: readonly ExecutionStatus[]): ExecutionRecord[] {
    if (!statuses || statuses.length === 0) return this.db.prepare("SELECT * FROM executions ORDER BY created_at_ms,id").all().map(executionFrom);
    const placeholders = statuses.map(() => "?").join(",");
    return this.db.prepare(`SELECT * FROM executions WHERE status IN (${placeholders}) ORDER BY created_at_ms,id`).all(...statuses).map(executionFrom);
  }

  executionMembers(executionId: string): ExecutionMember[] {
    return this.db.prepare("SELECT * FROM execution_members WHERE execution_id=? ORDER BY member_index")
      .all(executionId).map(executionMemberFrom);
  }

  activeExecutionForJob(jobId: string): ExecutionRecord | undefined {
    const placeholders = IN_FLIGHT.map(() => "?").join(",");
    const row = this.db.prepare(`SELECT executions.* FROM executions
      JOIN execution_members ON execution_members.execution_id=executions.id
      WHERE execution_members.job_id=? AND executions.status IN (${placeholders})
      ORDER BY executions.created_at_ms DESC LIMIT 1`).get(jobId, ...IN_FLIGHT);
    return row ? executionFrom(row) : undefined;
  }

  claimQueuedGroup(
    jobIds: readonly string[], executionId: string, workerId: string,
    requestJson: string, predictedDurationMs: number,
    strategyId = "", strategyVersion = 0, batchPlanJson = "",
  ): ExecutionRecord | undefined {
    if (jobIds.length === 0) return undefined;
    return this.transaction(() => {
      const busyPlaceholders = IN_FLIGHT.map(() => "?").join(",");
      const workerBusy = this.db.prepare(`SELECT 1 FROM executions WHERE worker_id=? AND status IN (${busyPlaceholders}) LIMIT 1`)
        .get(workerId, ...IN_FLIGHT);
      if (workerBusy) return undefined;
      const idPlaceholders = jobIds.map(() => "?").join(",");
      const queued = num(this.db.prepare(`SELECT COUNT(*) AS count FROM jobs WHERE id IN (${idPlaceholders}) AND status='queued'`).get(...jobIds)?.count);
      if (queued !== jobIds.length) return undefined;
      const now = Date.now();
      this.db.prepare(`INSERT INTO executions(
        id,worker_id,request_json,status,predicted_duration_ms,batch_size,created_at_ms,updated_at_ms,strategy_id,strategy_version,batch_plan_json
      ) VALUES(?,?,?,'dispatching',?,?,?, ?,?,?,?)`).run(
        executionId, workerId, requestJson, predictedDurationMs, jobIds.length, now, now,
        strategyId, strategyVersion, batchPlanJson,
      );
      const insertMember = this.db.prepare("INSERT INTO execution_members(execution_id,job_id,member_index) VALUES(?,?,?)");
      const updateJob = this.db.prepare(`UPDATE jobs SET status='dispatching',worker_id=?,backend_prompt_id=?,predicted_duration_ms=?,
        error_code='',error_message='',batch_blocked_adapter='',updated_at_ms=? WHERE id=? AND status='queued'`);
      const perMemberPrediction = Math.max(1, Math.round(predictedDurationMs / jobIds.length));
      for (let index = 0; index < jobIds.length; index += 1) {
        const jobId = jobIds[index]!;
        insertMember.run(executionId, jobId, index);
        if (updateJob.run(workerId, executionId, perMemberPrediction, now, jobId).changes !== 1) throw new Error(`failed to lease queued job ${jobId}`);
      }
      return this.getExecution(executionId);
    });
  }

  markExecutionSubmitted(id: string, backendId: string, backendResponseJson: string): void {
    this.transaction(() => {
      const now = Date.now();
      this.db.prepare(`UPDATE executions SET status='submitted',backend_prompt_id=?,backend_response_json=?,
        submitted_at_ms=COALESCE(submitted_at_ms,?),updated_at_ms=? WHERE id=? AND status='dispatching'`)
        .run(backendId, backendResponseJson, now, now, id);
      this.db.prepare(`UPDATE jobs SET status='submitted',backend_prompt_id=?,backend_response_json=?,
        submitted_at_ms=COALESCE(submitted_at_ms,?),updated_at_ms=? WHERE id IN (
          SELECT job_id FROM execution_members WHERE execution_id=?
        ) AND status='dispatching'`).run(backendId, backendResponseJson, now, now, id);
    });
  }

  markExecutionRunning(id: string): void {
    this.transaction(() => {
      const now = Date.now();
      this.db.prepare("UPDATE executions SET status='running',updated_at_ms=? WHERE id=? AND status IN ('submitted','running')").run(now, id);
      this.db.prepare(`UPDATE jobs SET status='running',updated_at_ms=? WHERE id IN (
        SELECT job_id FROM execution_members WHERE execution_id=?
      ) AND status IN ('submitted','running')`).run(now, id);
    });
  }

  markExecutionCollecting(id: string): void {
    this.transaction(() => {
      const now = Date.now();
      this.db.prepare("UPDATE executions SET status='collecting',updated_at_ms=? WHERE id=? AND status IN ('submitted','running','uncertain','collecting')").run(now, id);
      this.db.prepare(`UPDATE jobs SET status='collecting',updated_at_ms=? WHERE id IN (
        SELECT job_id FROM execution_members WHERE execution_id=?
      ) AND status IN ('submitted','running','uncertain','collecting')`).run(now, id);
    });
  }

  markExecutionUncertain(id: string, detail: string, errorCode = "submission_uncertain"): void {
    this.transaction(() => {
      const now = Date.now();
      this.db.prepare(`UPDATE executions SET status='uncertain',error_code=?,error_message=?,updated_at_ms=?
        WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')`).run(errorCode, detail, now, id);
      this.db.prepare(`UPDATE jobs SET status='uncertain',error_code=?,error_message=?,updated_at_ms=? WHERE id IN (
        SELECT job_id FROM execution_members WHERE execution_id=?
      ) AND status NOT IN ('succeeded','failed','cancelled')`).run(errorCode, detail, now, id);
    });
  }

  markExecutionTerminal(id: string, status: "succeeded" | "failed" | "cancelled", errorCode = "", errorMessage = ""): void {
    const now = Date.now();
    this.db.prepare(`UPDATE executions SET status=?,error_code=?,error_message=?,updated_at_ms=?,completed_at_ms=?
      WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')`).run(status, errorCode, errorMessage, now, now, id);
  }

  requeueBatchExecution(id: string, detail: string): number {
    return this.transaction(() => {
      const execution = this.getExecution(id);
      if (!execution || execution.batchSize < 2) return 0;
      const now = Date.now();
      this.db.prepare(`UPDATE executions SET status='failed',error_code='batch_fallback',error_message=?,updated_at_ms=?,completed_at_ms=?
        WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')`).run(detail, now, now, id);
      const adapterId = execution.strategyId;
      return Number(this.db.prepare(`UPDATE jobs SET status='queued',worker_id='',backend_prompt_id='',backend_response_json='',
        predicted_duration_ms=0,error_code='',error_message=?,submitted_at_ms=NULL,completed_at_ms=NULL,batch_blocked=1,batch_blocked_adapter=?,updated_at_ms=?
        WHERE id IN (SELECT job_id FROM execution_members WHERE execution_id=?)
        AND status NOT IN ('succeeded','failed','cancelled')`).run(`physical batch fell back to single execution: ${detail}`, adapterId, now, id).changes);
    });
  }

  cancelExecutionMember(jobId: string): boolean {
    const now = Date.now();
    return this.db.prepare(`UPDATE jobs SET status='cancelled',error_code='cancelled',error_message='batch member cancelled',
      history_json=?,completed_at_ms=?,updated_at_ms=? WHERE id=? AND status IN ('dispatching','submitted','running','collecting','uncertain')`)
      .run(JSON.stringify(errorHistory(jobId, "batch member cancelled")), now, now, jobId).changes === 1;
  }

  nonTerminalExecutionMembers(executionId: string): ExecutionMember[] {
    return this.db.prepare(`SELECT execution_members.* FROM execution_members JOIN jobs ON jobs.id=execution_members.job_id
      WHERE execution_members.execution_id=? AND jobs.status NOT IN ('succeeded','failed','cancelled') ORDER BY member_index`)
      .all(executionId).map(executionMemberFrom);
  }

  claimQueued(id: string, workerId: string, predictedDurationMs: number): JobRecord | undefined {
    return this.transaction(() => {
      const busyPlaceholders = IN_FLIGHT.map(() => "?").join(",");
      const row = this.db.prepare(`SELECT * FROM jobs WHERE id=? AND status='queued' AND NOT EXISTS (
        SELECT 1 FROM jobs active WHERE active.worker_id=? AND active.status IN (${busyPlaceholders})
      )`).get(id, workerId, ...IN_FLIGHT);
      if (!row) return undefined;
      const changed = this.db.prepare("UPDATE jobs SET status='dispatching',worker_id=?,predicted_duration_ms=?,updated_at_ms=? WHERE id=? AND status='queued'")
        .run(workerId, predictedDurationMs, Date.now(), id).changes;
      return changed === 1 ? this.getJob(id) : undefined;
    });
  }

  markSubmitted(id: string, backendId: string, backendResponseJson: string): void {
    const now = Date.now();
    this.db.prepare("UPDATE jobs SET status='submitted',backend_prompt_id=?,backend_response_json=?,submitted_at_ms=COALESCE(submitted_at_ms,?),updated_at_ms=? WHERE id=? AND status='dispatching'")
      .run(backendId, backendResponseJson, now, now, id);
  }

  markRunning(id: string): void {
    this.db.prepare("UPDATE jobs SET status='running',updated_at_ms=? WHERE id=? AND status IN ('submitted','running')").run(Date.now(), id);
  }

  resetQueued(id: string, message = ""): void {
    this.db.prepare("UPDATE jobs SET status='queued',worker_id='',backend_prompt_id='',backend_response_json='',error_message=?,updated_at_ms=? WHERE id=? AND status='dispatching'")
      .run(message, Date.now(), id);
  }

  markUncertain(id: string, message: string): void {
    this.db.prepare("UPDATE jobs SET status='uncertain',error_code='submission_uncertain',error_message=?,updated_at_ms=? WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')")
      .run(message, Date.now(), id);
  }

  markInterruptRequested(id: string): void {
    this.db.prepare("UPDATE jobs SET status='uncertain',error_code='interrupt_requested',error_message='interrupt requested; awaiting backend terminal state',updated_at_ms=? WHERE id=? AND status IN ('submitted','running','uncertain')")
      .run(Date.now(), id);
  }

  markTerminal(id: string, status: "succeeded" | "failed" | "cancelled", historyJson: string, errorCode = "", errorMessage = ""): void {
    const now = Date.now();
    this.db.prepare("UPDATE jobs SET status=?,history_json=?,error_code=?,error_message=?,updated_at_ms=?,completed_at_ms=? WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')")
      .run(status, historyJson, errorCode, errorMessage, now, now, id);
  }

  markCollecting(id: string): void {
    this.db.prepare("UPDATE jobs SET status='collecting',updated_at_ms=? WHERE id=? AND status IN ('submitted','running','uncertain')").run(Date.now(), id);
  }

  cancelQueued(id: string): boolean {
    const now = Date.now();
    return this.db.prepare("UPDATE jobs SET status='cancelled',error_code='cancelled',error_message='job cancelled before dispatch',history_json=?,completed_at_ms=?,updated_at_ms=? WHERE id=? AND status='queued'")
      .run(JSON.stringify(errorHistory(id, "job cancelled before dispatch")), now, now, id).changes === 1;
  }

  saveOutputs(id: string, historyJson: string, outputs: readonly OutputRecord[]): void {
    this.transaction(() => {
      this.db.prepare("DELETE FROM outputs WHERE job_id=?").run(id);
      const insert = this.db.prepare("INSERT INTO outputs(job_id,node_id,ordinal,filename,subfolder,type,relative_path,content_type,size_bytes) VALUES(?,?,?,?,?,?,?,?,?)");
      for (const output of outputs) insert.run(output.jobId, output.nodeId, output.ordinal, output.filename, output.subfolder, output.type, output.relativePath, output.contentType, output.sizeBytes);
      this.markTerminal(id, "succeeded", historyJson);
    });
  }

  saveExecutionOutputs(
    executionId: string,
    members: readonly { jobId: string; historyJson: string; outputs: readonly OutputRecord[] }[],
  ): void {
    this.transaction(() => {
      const insert = this.db.prepare("INSERT INTO outputs(job_id,node_id,ordinal,filename,subfolder,type,relative_path,content_type,size_bytes) VALUES(?,?,?,?,?,?,?,?,?)");
      for (const member of members) {
        const job = this.getJob(member.jobId);
        if (!job || ["succeeded", "failed", "cancelled"].includes(job.status)) continue;
        this.db.prepare("DELETE FROM outputs WHERE job_id=?").run(member.jobId);
        for (const output of member.outputs) {
          insert.run(output.jobId, output.nodeId, output.ordinal, output.filename, output.subfolder, output.type, output.relativePath, output.contentType, output.sizeBytes);
        }
        this.markTerminal(member.jobId, "succeeded", member.historyJson);
      }
      this.markExecutionTerminal(executionId, "succeeded");
    });
  }

  findOutput(filename: string, subfolder: string, type: string): OutputRecord | undefined {
    const row = this.db.prepare("SELECT * FROM outputs WHERE filename=? AND subfolder=? AND type=?").get(filename, subfolder, type);
    if (!row) return undefined;
    return { jobId: str(row.job_id), nodeId: str(row.node_id), ordinal: num(row.ordinal), filename: str(row.filename), subfolder: str(row.subfolder), type: str(row.type), relativePath: str(row.relative_path), contentType: str(row.content_type), sizeBytes: num(row.size_bytes) };
  }

  countInFlight(): number {
    const placeholders = IN_FLIGHT.map(() => "?").join(",");
    return num(this.db.prepare(`SELECT COUNT(*) AS count FROM jobs WHERE status IN (${placeholders})`).get(...IN_FLIGHT)?.count);
  }

  workerStats(workerId: string): { ewmaMs: number; samples: number } {
    const row = this.db.prepare("SELECT ewma_ms,samples FROM worker_stats WHERE worker_id=?").get(workerId);
    return { ewmaMs: num(row?.ewma_ms), samples: num(row?.samples) };
  }

  observeWorker(workerId: string, elapsedMs: number, alpha: number): { ewmaMs: number; samples: number } {
    const old = this.workerStats(workerId);
    const ewmaMs = old.samples === 0 ? elapsedMs : alpha * elapsedMs + (1 - alpha) * old.ewmaMs;
    const samples = old.samples + 1;
    this.db.prepare(`INSERT INTO worker_stats(worker_id,ewma_ms,samples,updated_at_ms) VALUES(?,?,?,?)
      ON CONFLICT(worker_id) DO UPDATE SET ewma_ms=excluded.ewma_ms,samples=excluded.samples,updated_at_ms=excluded.updated_at_ms`)
      .run(workerId, ewmaMs, samples, Date.now());
    return { ewmaMs, samples };
  }

  modelStats(workerId: string, modelId: string): { ewmaMs: number; samples: number } {
    const row = this.db.prepare("SELECT ewma_ms,samples FROM worker_model_stats WHERE worker_id=? AND model_id=?").get(workerId, modelId);
    return { ewmaMs: num(row?.ewma_ms), samples: num(row?.samples) };
  }

  observeWorkerModel(workerId: string, modelId: string, elapsedMs: number, alpha: number): { ewmaMs: number; samples: number } {
    const old = this.modelStats(workerId, modelId);
    const ewmaMs = old.samples === 0 ? elapsedMs : alpha * elapsedMs + (1 - alpha) * old.ewmaMs;
    const samples = old.samples + 1;
    this.db.prepare(`INSERT INTO worker_model_stats(worker_id,model_id,ewma_ms,samples,updated_at_ms) VALUES(?,?,?,?,?)
      ON CONFLICT(worker_id,model_id) DO UPDATE SET ewma_ms=excluded.ewma_ms,samples=excluded.samples,updated_at_ms=excluded.updated_at_ms`)
      .run(workerId, modelId, ewmaMs, samples, Date.now());
    return { ewmaMs, samples };
  }

  catalogRevision(): number { return num(this.db.prepare("SELECT current_revision AS revision FROM catalog_state WHERE singleton=1").get()?.revision); }
  advanceCatalogRevision(): number {
    this.db.prepare("UPDATE catalog_state SET current_revision=current_revision+1,updated_at_ms=? WHERE singleton=1").run(Date.now());
    return this.catalogRevision();
  }

  setWorkerCatalog(workerId: string, revision: number, state: string, lastError = ""): void {
    this.db.prepare(`INSERT INTO worker_catalog(worker_id,applied_revision,state,last_error,updated_at_ms) VALUES(?,?,?,?,?)
      ON CONFLICT(worker_id) DO UPDATE SET applied_revision=excluded.applied_revision,state=excluded.state,last_error=excluded.last_error,updated_at_ms=excluded.updated_at_ms`)
      .run(workerId, revision, state, lastError, Date.now());
  }

  terminalStorage(): Array<{ id: string; completedAtMs: number; sizeBytes: number }> {
    return this.db.prepare(`SELECT jobs.id,jobs.completed_at_ms,COALESCE(SUM(outputs.size_bytes),0) AS size_bytes
      FROM jobs LEFT JOIN outputs ON outputs.job_id=jobs.id
      WHERE jobs.status IN ('succeeded','failed','cancelled') AND jobs.completed_at_ms IS NOT NULL
      GROUP BY jobs.id,jobs.completed_at_ms ORDER BY jobs.completed_at_ms`).all().map((row) => ({ id: str(row.id), completedAtMs: num(row.completed_at_ms), sizeBytes: num(row.size_bytes) }));
  }

  deleteTerminalJob(id: string): boolean {
    return this.transaction(() => {
      const changed = this.db.prepare("DELETE FROM jobs WHERE id=? AND status IN ('succeeded','failed','cancelled')").run(id).changes === 1;
      if (changed) this.db.prepare(`DELETE FROM executions WHERE status IN ('succeeded','failed','cancelled')
        AND NOT EXISTS (SELECT 1 FROM execution_members WHERE execution_members.execution_id=executions.id)`).run();
      return changed;
    });
  }
}

export function errorHistory(id: string, message: string): Record<string, unknown> {
  return { [id]: { outputs: {}, status: { status_str: "error", completed: true, messages: [["execution_error", { prompt_id: id, exception_message: message }]] } } };
}
