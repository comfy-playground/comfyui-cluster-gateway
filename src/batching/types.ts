import type { JsonObject, PromptEnvelope } from "../types.js";

export type BatchRejectReason =
  | "multiple_samplers"
  | "multiple_outputs"
  | "unsupported_loader"
  | "unsupported_sampler"
  | "unsupported_scheduler"
  | "missing_seed"
  | "invalid_latent"
  | "extra_seed_input"
  | "backend_capability_missing";

export interface BatchRejection {
  kind: "reject";
  reason: BatchRejectReason;
  detail: string;
}

export interface BatchCandidate<TPayload extends JsonObject = JsonObject> {
  adapterId: string;
  adapterVersion: number;
  mergeKey: string;
  seed: string;
  payload: TPayload;
}

export type BatchAssessment<TPayload extends JsonObject = JsonObject> =
  | { kind: "candidate"; candidate: BatchCandidate<TPayload> }
  | BatchRejection;

export interface BatchPlan extends JsonObject {
  adapter_id: string;
  adapter_version: number;
  output: JsonObject;
}

export interface PhysicalBatch {
  envelope: PromptEnvelope;
  batchSize: number;
  plan: BatchPlan;
}

export interface BatchAdapter {
  readonly id: string;
  readonly version: number;
  readonly maxBatchSize: number;
  readonly requiredBackendNodes: readonly string[];

  assess(envelope: PromptEnvelope): BatchAssessment;
  merge(candidates: readonly BatchCandidate[], executionId: string): PhysicalBatch;
  splitHistory(history: JsonObject, plan: BatchPlan, executionId: string, jobId: string, memberIndex: number): JsonObject;
}
