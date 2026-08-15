import type { PromptEnvelope } from "../types.js";
import type { BatchAdapter, BatchAssessment, BatchCandidate } from "./types.js";

export class BatchAdapterRegistry {
  private readonly byId: Map<string, BatchAdapter>;

  constructor(readonly adapters: readonly BatchAdapter[]) {
    this.byId = new Map();
    for (const adapter of adapters) {
      if (this.byId.has(adapter.id)) throw new Error(`duplicate batch adapter ${adapter.id}`);
      this.byId.set(adapter.id, adapter);
    }
  }

  get(id: string): BatchAdapter | undefined { return this.byId.get(id); }

  assess(envelope: PromptEnvelope): { candidate?: BatchCandidate; rejections: BatchAssessment[] } {
    const rejections: BatchAssessment[] = [];
    for (const adapter of this.adapters) {
      const assessment = adapter.assess(envelope);
      if (assessment.kind === "candidate") return { candidate: assessment.candidate, rejections };
      rejections.push(assessment);
    }
    return { rejections };
  }

  compatible(adapter: BatchAdapter, availableNodes: ReadonlySet<string>): boolean {
    return adapter.requiredBackendNodes.every((node) => availableNodes.has(node));
  }
}

