import type { PromptEnvelope } from "./types.js";
import { createBatchAdapters } from "./batching/adapters.js";
import type { BatchCandidate } from "./batching/types.js";

export type { BatchCandidate } from "./batching/types.js";

const legacyAdapter = createBatchAdapters().find((adapter) => adapter.id === "sdxl-euler-normal")!;

/**
 * Compatibility wrapper for benchmark scripts and callers that only support
 * the original SDXL batch contract. New code should use BatchAdapterRegistry.
 */
export function analyzeBatchCandidate(envelope: PromptEnvelope): BatchCandidate | undefined {
  const result = legacyAdapter.assess(envelope);
  return result.kind === "candidate" ? result.candidate : undefined;
}

/** Compatibility wrapper for the original SDXL-only graph API. */
export function mergeBatchCandidates(candidates: readonly BatchCandidate[], executionId: string): PromptEnvelope {
  return legacyAdapter.merge(candidates, executionId).envelope;
}

