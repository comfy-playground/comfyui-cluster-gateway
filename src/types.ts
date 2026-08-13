export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export interface JsonObject { [key: string]: JsonValue }

export interface WorkerConfig {
  id: string;
  url: string;
  expectedDeviceName: string;
  secondsPerImage: number;
  capabilities: string[];
  required: boolean;
  primary: boolean;
  enabled: boolean;
}

export interface GatewayConfig {
  version: 1;
  listen: { host: string; port: number };
  databasePath: string;
  outputDirectory: string;
  workers: WorkerConfig[];
  timeouts: {
    workerRequestMs: number;
    historyPollMs: number;
    maxJobRuntimeMs: number;
    catalogDrainMs: number;
  };
  limits: {
    maxPromptBytes: number;
    maxOutputBytes: number;
    maxOutputsPerJob: number;
    maxQueuedJobs: number;
  };
  scheduler: {
    ewmaAlpha: number;
    ewmaMinSamples: number;
    agingSeconds: number;
    tieEpsilonMs: number;
  };
  catalog: { refreshOnStart: boolean; retryMs: number; pageSize: number };
  retention: {
    maxAgeHours: number;
    maxTotalBytes: number;
    minAgeHours: number;
    sweepIntervalMs: number;
  };
  auth: { generationToken: string; managementToken: string };
}

export type JobStatus =
  | "queued" | "dispatching" | "submitted" | "running" | "collecting"
  | "succeeded" | "failed" | "cancelled" | "uncertain";

export interface JobRecord {
  number: number;
  id: string;
  clientId: string;
  requestJson: string;
  status: JobStatus;
  workerId: string;
  backendPromptId: string;
  backendResponseJson: string;
  historyJson: string;
  predictedDurationMs: number;
  errorCode: string;
  errorMessage: string;
  createdAtMs: number;
  updatedAtMs: number;
  submittedAtMs: number | null;
  completedAtMs: number | null;
}

export interface OutputRecord {
  jobId: string;
  nodeId: string;
  ordinal: number;
  filename: string;
  subfolder: string;
  type: string;
  relativePath: string;
  contentType: string;
  sizeBytes: number;
}

export type WorkerState = "disabled" | "offline" | "syncing" | "stale" | "ready" | "busy" | "draining";

export interface WorkerSnapshot {
  id: string;
  state: WorkerState;
  ready: boolean;
  busy: boolean;
  required: boolean;
  primary: boolean;
  currentJobId: string;
  appliedRevision: number;
  secondsPerImage: number;
  effectiveSecondsPerImage: number;
  ewmaSamples: number;
  lastError: string;
  deviceName: string;
}

export interface PromptEnvelope extends JsonObject {
  prompt: JsonObject;
  client_id?: string;
  prompt_id?: string;
  extra_data?: JsonObject;
}

export interface BackendImage {
  filename: string;
  subfolder?: string;
  type?: string;
}
