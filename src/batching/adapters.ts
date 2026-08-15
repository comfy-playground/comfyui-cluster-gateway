import type { JsonObject, PromptEnvelope } from "../types.js";
import { analyzeKSamplerGraph, mergeKSamplerGraphs, splitKSamplerHistory } from "./graph.js";
import type { BatchAdapter, BatchAssessment, BatchCandidate, BatchPlan, PhysicalBatch } from "./types.js";

interface KSamplerAdapterOptions {
  id: string;
  version: number;
  maxBatchSize: number;
  loaderClasses: readonly string[];
  samplerNames: readonly string[];
  schedulers: readonly string[];
  requiredBackendNodes?: readonly string[];
  mergePolicy?: { samplerClassType?: string; seedInput?: boolean };
}

interface KSamplerPayload extends JsonObject {
  envelope: PromptEnvelope;
  samplerNodeId: string;
  latentNodeId: string;
  saveNodeId: string;
  samplerName: string;
  scheduler: string;
}

class KSamplerBatchAdapter implements BatchAdapter {
  readonly id: string;
  readonly version: number;
  readonly maxBatchSize: number;
  readonly requiredBackendNodes: readonly string[];
  private readonly options: KSamplerAdapterOptions;

  constructor(options: KSamplerAdapterOptions) {
    this.options = options;
    this.id = options.id;
    this.version = options.version;
    this.maxBatchSize = options.maxBatchSize;
    this.requiredBackendNodes = options.requiredBackendNodes ?? ["GatewayMultiSeedNoise"];
  }

  assess(envelope: PromptEnvelope): BatchAssessment {
    const result = analyzeKSamplerGraph(envelope, this.options);
    if ("kind" in result) return result;
    const { graph, mergeKey } = result;
    const payload: KSamplerPayload = {
      envelope,
      samplerNodeId: graph.samplerNodeId,
      latentNodeId: graph.latentNodeId,
      saveNodeId: graph.saveNodeId,
      samplerName: graph.samplerName,
      scheduler: graph.scheduler,
    };
    const candidate: BatchCandidate<KSamplerPayload> = {
      adapterId: this.id,
      adapterVersion: this.version,
      mergeKey,
      seed: graph.seed,
      payload,
    };
    return { kind: "candidate", candidate };
  }

  merge(candidates: readonly BatchCandidate[], executionId: string): PhysicalBatch {
    return mergeKSamplerGraphs(candidates, executionId, this.maxBatchSize, this.options.mergePolicy);
  }

  splitHistory(history: JsonObject, plan: BatchPlan, executionId: string, jobId: string, memberIndex: number): JsonObject {
    return splitKSamplerHistory(history, plan, executionId, jobId, memberIndex);
  }
}

export function createBatchAdapters(): BatchAdapter[] {
  return [
    new KSamplerBatchAdapter({
      id: "sdxl-euler-normal", version: 1, maxBatchSize: 16,
      loaderClasses: ["CheckpointLoaderSimple"], samplerNames: ["euler"], schedulers: ["normal"],
    }),
    new KSamplerBatchAdapter({
      id: "anima-euler-sgm", version: 1, maxBatchSize: 16,
      loaderClasses: ["UNETLoader"], samplerNames: ["euler"], schedulers: ["sgm_uniform"],
    }),
    new KSamplerBatchAdapter({
      id: "anima-er-sde", version: 1, maxBatchSize: 4,
      loaderClasses: ["UNETLoader"], samplerNames: ["er_sde"], schedulers: ["sgm_uniform"],
      requiredBackendNodes: ["GatewayMultiSeedStochasticSampler"],
      mergePolicy: { samplerClassType: "GatewayMultiSeedStochasticSampler", seedInput: true },
    }),
  ];
}
