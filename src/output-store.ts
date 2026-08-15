import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { dirname, extname, join, relative, resolve, sep } from "node:path";
import type { GatewayDatabase } from "./database.js";
import type { JsonObject, OutputRecord } from "./types.js";
import type { BatchAdapter, BatchPlan } from "./batching/types.js";
import { readResponseBytes, type WorkerClient } from "./worker-client.js";

const SAFE_IMAGE_EXTENSIONS = new Set([".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff", ".avif"]);

function safeExtension(filename: string): string {
  const extension = extname(filename).toLowerCase();
  return SAFE_IMAGE_EXTENSIONS.has(extension) ? extension : ".bin";
}

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}

function rewritePromptIds(value: unknown, executionId: string, jobId: string): void {
  if (Array.isArray(value)) {
    for (const item of value) rewritePromptIds(item, executionId, jobId);
    return;
  }
  const valueObject = object(value);
  if (!valueObject) return;
  for (const [key, child] of Object.entries(valueObject)) {
    if (key === "prompt_id" && child === executionId) valueObject[key] = jobId;
    else rewritePromptIds(child, executionId, jobId);
  }
}

export function historyForMember(history: JsonObject, executionId: string, jobId: string): JsonObject {
  const entry = object(history[executionId]);
  if (!entry) throw new Error("terminal history has no execution entry");
  const memberEntry = structuredClone(entry);
  rewritePromptIds(memberEntry, executionId, jobId);
  return { [jobId]: memberEntry } as JsonObject;
}

export interface CollectedExecutionMember {
  jobId: string;
  history: JsonObject;
  outputs: OutputRecord[];
}

export class OutputStore {
  constructor(
    private readonly root: string,
    private readonly database: GatewayDatabase,
    private readonly maxOutputBytes: number,
    private readonly maxOutputsPerJob: number,
  ) {}

  async initialize(): Promise<void> { await mkdir(this.root, { recursive: true, mode: 0o750 }); }

  async collect(jobId: string, client: WorkerClient, history: JsonObject): Promise<{ history: JsonObject; outputs: OutputRecord[] }> {
    if (!/^[0-9a-f-]{36}$/i.test(jobId)) throw new Error("unsafe job id");
    const root = structuredClone(history) as JsonObject;
    const entry = object(root[jobId]);
    if (!entry) throw new Error("terminal history has no prompt entry");
    const outputsObject = object(entry.outputs);
    if (!outputsObject) return { history: root, outputs: [] };
    const directory = join(this.root, jobId);
    await rm(directory, { recursive: true, force: true });
    await mkdir(directory, { recursive: true, mode: 0o750 });
    const records: OutputRecord[] = [];
    try {
      for (const nodeId of Object.keys(outputsObject).sort()) {
        const nodeOutput = object(outputsObject[nodeId]);
        if (!nodeOutput || !Array.isArray(nodeOutput.images)) continue;
        for (let ordinal = 0; ordinal < nodeOutput.images.length; ordinal += 1) {
          if (records.length >= this.maxOutputsPerJob) throw new Error(`output count exceeds ${this.maxOutputsPerJob}`);
          const image = object(nodeOutput.images[ordinal]);
          if (!image || typeof image.filename !== "string" || image.filename === "") throw new Error(`node ${nodeId} image ${ordinal} has no filename`);
          const backendFilename = image.filename;
          const backendSubfolder = typeof image.subfolder === "string" ? image.subfolder : "";
          const backendType = typeof image.type === "string" && image.type !== "" ? image.type : "output";
          const publicFilename = `${String(records.length).padStart(3, "0")}${safeExtension(backendFilename)}`;
          const query = new URLSearchParams({ filename: backendFilename, subfolder: backendSubfolder, type: backendType });
          const response = await client.request(`/view?${query.toString()}`);
          if (!response.ok) throw new Error(`backend view returned HTTP ${response.status}`);
          const contentType = (response.headers.get("content-type") ?? "").split(";", 1)[0]?.trim() ?? "";
          if (!contentType.startsWith("image/")) throw new Error(`backend view returned non-image content type ${contentType}`);
          const bytes = await readResponseBytes(response, this.maxOutputBytes);
          const finalPath = join(directory, publicFilename);
          const temporaryPath = `${finalPath}.collecting`;
          await writeFile(temporaryPath, bytes, { mode: 0o640, flag: "wx" });
          await rename(temporaryPath, finalPath);
          image.filename = publicFilename;
          image.subfolder = jobId;
          image.type = "output";
          records.push({ jobId, nodeId, ordinal, filename: publicFilename, subfolder: jobId, type: "output", relativePath: `${jobId}/${publicFilename}`, contentType, sizeBytes: bytes.byteLength });
        }
      }
      return { history: root, outputs: records };
    } catch (error) {
      await rm(directory, { recursive: true, force: true });
      throw error;
    }
  }

  async collectExecution(
    executionId: string,
    members: readonly { jobId: string; memberIndex: number }[],
    client: WorkerClient,
    history: JsonObject,
    adapter?: BatchAdapter,
    plan?: BatchPlan,
  ): Promise<CollectedExecutionMember[]> {
    if (!/^[0-9a-f-]{36}$/i.test(executionId)) throw new Error("unsafe execution id");
    if (members.some((member) => !/^[0-9a-f-]{36}$/i.test(member.jobId))) throw new Error("unsafe member job id");
    const executionEntry = object(history[executionId]);
    if (!executionEntry) throw new Error("terminal history has no execution entry");
    const createdDirectories: string[] = [];
    const results: CollectedExecutionMember[] = [];
    try {
      for (const member of members) {
        const memberHistory = adapter && plan
          ? adapter.splitHistory(history, plan, executionId, member.jobId, member.memberIndex)
          : historyForMember(history, executionId, member.jobId);
        const memberEntry = object(memberHistory[member.jobId])!;
        const outputsObject = object(memberEntry.outputs);
        const directory = join(this.root, member.jobId);
        await rm(directory, { recursive: true, force: true });
        await mkdir(directory, { recursive: true, mode: 0o750 });
        createdDirectories.push(directory);
        const records: OutputRecord[] = [];
        if (outputsObject) {
          for (const nodeId of Object.keys(outputsObject).sort()) {
            const nodeOutput = object(outputsObject[nodeId]);
            if (!nodeOutput || !Array.isArray(nodeOutput.images)) continue;
            const image = object(nodeOutput.images[adapter && plan ? 0 : member.memberIndex]);
            if (!image || typeof image.filename !== "string" || image.filename === "") {
              throw new Error(`node ${nodeId} has no image for batch member ${member.memberIndex}`);
            }
            if (records.length >= this.maxOutputsPerJob) throw new Error(`output count exceeds ${this.maxOutputsPerJob}`);
            const backendFilename = image.filename;
            const backendSubfolder = typeof image.subfolder === "string" ? image.subfolder : "";
            const backendType = typeof image.type === "string" && image.type !== "" ? image.type : "output";
            const publicFilename = `${String(records.length).padStart(3, "0")}${safeExtension(backendFilename)}`;
            const query = new URLSearchParams({ filename: backendFilename, subfolder: backendSubfolder, type: backendType });
            const response = await client.request(`/view?${query.toString()}`);
            if (!response.ok) throw new Error(`backend view returned HTTP ${response.status}`);
            const contentType = (response.headers.get("content-type") ?? "").split(";", 1)[0]?.trim() ?? "";
            if (!contentType.startsWith("image/")) throw new Error(`backend view returned non-image content type ${contentType}`);
            const bytes = await readResponseBytes(response, this.maxOutputBytes);
            const finalPath = join(directory, publicFilename);
            const temporaryPath = `${finalPath}.collecting`;
            await writeFile(temporaryPath, bytes, { mode: 0o640, flag: "wx" });
            await rename(temporaryPath, finalPath);
            image.filename = publicFilename;
            image.subfolder = member.jobId;
            image.type = "output";
            nodeOutput.images = [image];
            records.push({
              jobId: member.jobId, nodeId, ordinal: 0, filename: publicFilename,
              subfolder: member.jobId, type: "output", relativePath: `${member.jobId}/${publicFilename}`,
              contentType, sizeBytes: bytes.byteLength,
            });
          }
        }
        results.push({ jobId: member.jobId, history: memberHistory, outputs: records });
      }
      return results;
    } catch (error) {
      await Promise.all(createdDirectories.map((directory) => rm(directory, { recursive: true, force: true })));
      throw error;
    }
  }

  resolveOutput(filename: string, subfolder: string, type: string): { record: OutputRecord; path: string } | undefined {
    if (!filename || !subfolder || type !== "output") return undefined;
    const record = this.database.findOutput(filename, subfolder, type);
    if (!record) return undefined;
    const path = resolve(this.root, record.relativePath);
    const root = resolve(this.root);
    const difference = relative(root, path);
    if (difference.startsWith(`..${sep}`) || difference === ".." || difference === "") throw new Error("stored output path escapes output root");
    return { record, path };
  }

  async sweep(policy: { maxAgeHours: number; maxTotalBytes: number; minAgeHours: number }): Promise<{ deletedJobs: number; deletedBytes: number }> {
    const now = Date.now();
    const maxAgeMs = policy.maxAgeHours * 60 * 60 * 1000;
    const minAgeMs = policy.minAgeHours * 60 * 60 * 1000;
    const rows = this.database.terminalStorage();
    let totalBytes = rows.reduce((sum, row) => sum + row.sizeBytes, 0);
    let deletedJobs = 0;
    let deletedBytes = 0;
    for (const row of rows) {
      const age = now - row.completedAtMs;
      if (age < maxAgeMs && (totalBytes <= policy.maxTotalBytes || age < minAgeMs)) continue;
      await rm(join(this.root, row.id), { recursive: true, force: true });
      if (this.database.deleteTerminalJob(row.id)) {
        totalBytes -= row.sizeBytes;
        deletedBytes += row.sizeBytes;
        deletedJobs += 1;
      }
    }
    return { deletedJobs, deletedBytes };
  }
}
