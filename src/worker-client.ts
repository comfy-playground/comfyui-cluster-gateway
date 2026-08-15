import type { JsonObject, PromptEnvelope } from "./types.js";

export class UpstreamHttpError extends Error {
  constructor(readonly status: number, readonly body: Uint8Array, readonly contentType: string) {
    super(`upstream returned HTTP ${status}`);
  }
}

export interface HistoryState {
  found: boolean;
  terminal: boolean;
  succeeded: boolean;
  history: JsonObject;
}

export async function readResponseBytes(response: Response, maximumBytes: number): Promise<Uint8Array> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maximumBytes) throw new Error(`upstream response exceeds ${maximumBytes} bytes`);
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximumBytes) { await reader.cancel(); throw new Error(`upstream response exceeds ${maximumBytes} bytes`); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; }
  return result;
}

function timeoutSignal(milliseconds: number): AbortSignal { return AbortSignal.timeout(milliseconds); }

export class WorkerClient {
  constructor(readonly baseUrl: string, private readonly requestTimeoutMs: number) {}

  private url(path: string): URL { return new URL(path, `${this.baseUrl.replace(/\/$/, "")}/`); }

  async request(path: string, init: RequestInit = {}, longRunning = false): Promise<Response> {
    const requestInit: RequestInit = { ...init, redirect: "manual" };
    if (!longRunning && !requestInit.signal) requestInit.signal = timeoutSignal(this.requestTimeoutMs);
    const response = await fetch(this.url(path), requestInit);
    return response;
  }

  async json(path: string, init: RequestInit = {}, longRunning = false): Promise<{ response: Response; value: JsonObject }> {
    const response = await this.request(path, init, longRunning);
    const bytes = await readResponseBytes(response, 8 * 1024 * 1024);
    if (!response.ok) throw new UpstreamHttpError(response.status, bytes, response.headers.get("content-type") ?? "application/json");
    let value: unknown;
    try { value = JSON.parse(new TextDecoder().decode(bytes)); }
    catch { throw new Error("upstream returned invalid JSON"); }
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("upstream JSON must be an object");
    return { response, value: value as JsonObject };
  }

  async systemStats(): Promise<JsonObject> { return (await this.json("/system_stats")).value; }

  async probeDevice(expectedName: string): Promise<{ deviceName: string; systemStats: JsonObject }> {
    const stats = await this.systemStats();
    const devices = stats.devices;
    if (!Array.isArray(devices) || devices.length !== 1) throw new Error(`expected exactly one visible device, got ${Array.isArray(devices) ? devices.length : 0}`);
    const device = devices[0];
    if (device === null || typeof device !== "object" || Array.isArray(device)) throw new Error("system_stats device is invalid");
    const name = (device as JsonObject).name;
    if (name !== expectedName) throw new Error(`expected device ${JSON.stringify(expectedName)}, got ${JSON.stringify(name)}`);
    return { deviceName: name, systemStats: stats };
  }

  async validateDevice(expectedName: string): Promise<string> {
    return (await this.probeDevice(expectedName)).deviceName;
  }

  async supportsNode(classType: string): Promise<boolean> {
    const objectInfo = (await this.json(`/object_info/${encodeURIComponent(classType)}`)).value;
    const definition = objectInfo[classType];
    return definition !== null && typeof definition === "object" && !Array.isArray(definition);
  }

  async submit(envelope: PromptEnvelope, promptId: string): Promise<JsonObject> {
    const body: PromptEnvelope = { ...envelope, prompt_id: promptId };
    return (await this.json("/prompt", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).value;
  }

  async history(promptId: string): Promise<HistoryState> {
    const history = (await this.json(`/history/${encodeURIComponent(promptId)}`)).value;
    const entry = history[promptId];
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) return { found: false, terminal: false, succeeded: false, history };
    const status = (entry as JsonObject).status;
    const statusObject = status !== null && typeof status === "object" && !Array.isArray(status) ? status as JsonObject : {};
    const statusString = statusObject.status_str;
    const completed = statusObject.completed === true;
    return { found: true, terminal: completed || statusString === "success" || statusString === "error", succeeded: statusString === "success" || (completed && statusString !== "error"), history };
  }

  async queueContains(promptId: string): Promise<boolean> {
    const queue = (await this.json("/queue")).value;
    for (const key of ["queue_running", "queue_pending"] as const) {
      const groups = queue[key];
      if (!Array.isArray(groups)) continue;
      for (const group of groups) if (Array.isArray(group) && group[1] === promptId) return true;
    }
    return false;
  }

  async interrupt(promptId: string): Promise<JsonObject> {
    return (await this.json("/interrupt", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ prompt_id: promptId }) })).value;
  }

  async fullRebuild(): Promise<JsonObject> {
    const result = (await this.json("/api/lm/loras/scan?full_rebuild=true")).value;
    if (result.status !== "success" && result.success !== true) throw new Error("LoRA full rebuild did not report success");
    return result;
  }

  async proxy(pathAndQuery: string, method: string, body: Uint8Array | undefined, contentType: string | undefined, longRunning: boolean): Promise<Response> {
    const headers = new Headers();
    if (contentType) headers.set("content-type", contentType);
    const init: RequestInit = { method, headers };
    if (body) init.body = body;
    return this.request(pathAndQuery, init, longRunning);
  }
}
