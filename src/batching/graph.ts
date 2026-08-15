import { createHash } from "node:crypto";
import type { JsonObject, JsonValue, PromptEnvelope } from "../types.js";
import { splitIndexedImageHistory } from "./history.js";
import type { BatchCandidate, BatchPlan, BatchRejection, PhysicalBatch } from "./types.js";

const MAX_SEED = 0xffffffffffffffffn;

interface PromptNode extends JsonObject {
  class_type: string;
  inputs: JsonObject;
}

export interface KSamplerGraph {
  samplerNodeId: string;
  sampler: PromptNode;
  latentNodeId: string;
  latent: PromptNode;
  saveNodeId: string;
  seed: string;
  samplerName: string;
  scheduler: string;
}

export interface KSamplerGraphPolicy {
  loaderClasses: readonly string[];
  samplerNames: readonly string[];
  schedulers: readonly string[];
}

export interface KSamplerMergePolicy {
  samplerClassType?: string;
  seedInput?: boolean;
}

function object(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
}

function node(value: unknown): PromptNode | undefined {
  const valueObject = object(value);
  const inputs = object(valueObject?.inputs);
  return valueObject && typeof valueObject.class_type === "string" && inputs
    ? valueObject as PromptNode
    : undefined;
}

function seedString(value: JsonValue | undefined): string | undefined {
  if (typeof value === "number") return Number.isSafeInteger(value) && value >= 0 ? String(value) : undefined;
  if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) return undefined;
  const parsed = BigInt(value);
  return parsed <= MAX_SEED ? value : undefined;
}

function inputLink(value: JsonValue | undefined): [string, number] | undefined {
  if (!Array.isArray(value) || value.length !== 2 || typeof value[0] !== "string" || typeof value[1] !== "number") return undefined;
  return [value[0], value[1]];
}

export function canonical(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key]!)}`).join(",")}}`;
}

function rejection(reason: BatchRejection["reason"], detail: string): BatchRejection {
  return { kind: "reject", reason, detail };
}

export function analyzeKSamplerGraph(envelope: PromptEnvelope, policy: KSamplerGraphPolicy):
  | { graph: KSamplerGraph; mergeKey: string }
  | BatchRejection {
  const entries = Object.entries(envelope.prompt);
  const samplers = entries.filter(([, value]) => node(value)?.class_type === "KSampler");
  const saves = entries.filter(([, value]) => node(value)?.class_type === "SaveImage");
  const loaders = entries.filter(([, value]) => policy.loaderClasses.includes(node(value)?.class_type ?? ""));
  if (samplers.length !== 1) return rejection("multiple_samplers", `expected one KSampler, got ${samplers.length}`);
  if (saves.length !== 1) return rejection("multiple_outputs", `expected one SaveImage, got ${saves.length}`);
  if (loaders.length !== 1) return rejection("unsupported_loader", "workflow loader is not supported by this adapter");

  const [samplerNodeId, samplerValue] = samplers[0]!;
  const [saveNodeId] = saves[0]!;
  const sampler = node(samplerValue)!;
  const samplerName = sampler.inputs.sampler_name;
  const scheduler = sampler.inputs.scheduler;
  if (typeof samplerName !== "string" || !policy.samplerNames.includes(samplerName)) {
    return rejection("unsupported_sampler", `sampler ${String(samplerName)} is not supported`);
  }
  if (typeof scheduler !== "string" || !policy.schedulers.includes(scheduler)) {
    return rejection("unsupported_scheduler", `scheduler ${String(scheduler)} is not supported`);
  }
  const seed = seedString(sampler.inputs.seed);
  const latentLink = inputLink(sampler.inputs.latent_image);
  if (!seed) return rejection("missing_seed", "KSampler seed must be an unsigned integer");
  if (!latentLink || latentLink[1] !== 0) return rejection("invalid_latent", "KSampler latent_image must link to output 0");
  const latentNodeId = latentLink[0];
  const latent = node(envelope.prompt[latentNodeId]);
  if (!latent || latent.class_type !== "EmptyLatentImage" || latent.inputs.batch_size !== 1) {
    return rejection("invalid_latent", "latent must be EmptyLatentImage with batch_size=1");
  }

  for (const [nodeId, value] of entries) {
    const promptNode = node(value);
    if (!promptNode) return rejection("unsupported_loader", `node ${nodeId} is not a valid prompt node`);
    for (const inputName of Object.keys(promptNode.inputs)) {
      if ((inputName === "seed" || inputName === "noise_seed") && !(nodeId === samplerNodeId && inputName === "seed")) {
        return rejection("extra_seed_input", `node ${nodeId} has an unsupported ${inputName} input`);
      }
    }
  }

  const normalized = structuredClone(envelope) as PromptEnvelope;
  delete normalized.client_id;
  delete normalized.prompt_id;
  const normalizedSampler = node(normalized.prompt[samplerNodeId])!;
  normalizedSampler.inputs.seed = "__gateway_member_seed__";
  const normalizedSave = node(normalized.prompt[saveNodeId])!;
  normalizedSave.inputs.filename_prefix = "__gateway_batch_output__";
  return {
    graph: { samplerNodeId, sampler, latentNodeId, latent, saveNodeId, seed, samplerName, scheduler },
    mergeKey: createHash("sha256").update(canonical(normalized)).digest("hex"),
  };
}

function nextNodeIds(prompt: JsonObject, count: number): string[] {
  const numericIds = Object.keys(prompt).map(Number).filter(Number.isSafeInteger);
  let next = numericIds.length > 0 ? Math.max(...numericIds) + 1 : 1;
  const result: string[] = [];
  while (result.length < count) {
    const candidate = String(next++);
    if (!(candidate in prompt)) result.push(candidate);
  }
  return result;
}

function requiredInput(inputs: JsonObject, name: string): JsonValue {
  const value = inputs[name];
  if (value === undefined) throw new Error(`KSampler input ${name} is missing`);
  return value;
}

export function mergeKSamplerGraphs(
  candidates: readonly BatchCandidate[],
  executionId: string,
  maxBatchSize: number,
  mergePolicy: KSamplerMergePolicy = {},
): PhysicalBatch {
  if (candidates.length < 2) throw new Error("a physical batch requires at least two members");
  if (candidates.length > maxBatchSize) throw new Error(`batch size ${candidates.length} exceeds adapter limit ${maxBatchSize}`);
  const first = candidates[0]!;
  if (candidates.some((candidate) => candidate.adapterId !== first.adapterId || candidate.mergeKey !== first.mergeKey)) {
    throw new Error("batch candidates do not have the same adapter or merge key");
  }
  const firstGraph = first.payload as JsonObject;
  const merged = structuredClone(firstGraph.envelope) as PromptEnvelope;
  const samplerNodeId = String(firstGraph.samplerNodeId);
  const latentNodeId = String(firstGraph.latentNodeId);
  const saveNodeId = String(firstGraph.saveNodeId);
  const sampler = node(merged.prompt[samplerNodeId]);
  const latent = node(merged.prompt[latentNodeId]);
  const save = node(merged.prompt[saveNodeId]);
  if (!sampler || !latent || !save) throw new Error("batch graph changed before merge");

  const model = requiredInput(sampler.inputs, "model");
  const positive = requiredInput(sampler.inputs, "positive");
  const negative = requiredInput(sampler.inputs, "negative");
  const cfg = requiredInput(sampler.inputs, "cfg");
  const steps = requiredInput(sampler.inputs, "steps");
  const denoise = requiredInput(sampler.inputs, "denoise");
  const latentImage = requiredInput(sampler.inputs, "latent_image");
  const samplerName = String(firstGraph.samplerName);
  const scheduler = String(firstGraph.scheduler);
  const newNodeIds = nextNodeIds(merged.prompt, 4);
  const noiseId = newNodeIds[0]!;
  const guiderId = newNodeIds[1]!;
  const samplerSelectId = newNodeIds[2]!;
  const schedulerId = newNodeIds[3]!;
  const samplerClassType = mergePolicy.samplerClassType ?? "SamplerCustomAdvanced";
  const seedInput = mergePolicy.seedInput ?? false;

  latent.inputs.batch_size = candidates.length;
  save.inputs.filename_prefix = `gateway_batch_${executionId}`;
  if (!seedInput) {
    merged.prompt[noiseId] = { class_type: "GatewayMultiSeedNoise", inputs: { seeds: JSON.stringify(candidates.map((candidate) => candidate.seed)) } };
  }
  merged.prompt[guiderId] = { class_type: "CFGGuider", inputs: { model, positive, negative, cfg } };
  merged.prompt[samplerSelectId] = { class_type: "KSamplerSelect", inputs: { sampler_name: samplerName } };
  merged.prompt[schedulerId] = { class_type: "BasicScheduler", inputs: { model, scheduler, steps, denoise } };
  const samplerInputs: JsonObject = {
    guider: [guiderId, 0], sampler: [samplerSelectId, 0], sigmas: [schedulerId, 0], latent_image: latentImage,
  };
  if (seedInput) samplerInputs.seeds = JSON.stringify(candidates.map((candidate) => candidate.seed));
  else samplerInputs.noise = [noiseId, 0];
  merged.prompt[samplerNodeId] = { class_type: samplerClassType, inputs: samplerInputs };
  delete merged.prompt_id;
  const plan: BatchPlan = {
    adapter_id: first.adapterId,
    adapter_version: first.adapterVersion,
    output: { kind: "indexed_images", node_ids: [saveNodeId] },
  };
  return { envelope: merged, batchSize: candidates.length, plan };
}

export function splitKSamplerHistory(history: JsonObject, plan: BatchPlan, executionId: string, jobId: string, memberIndex: number): JsonObject {
  if (plan.output.kind !== "indexed_images") throw new Error(`unsupported batch output plan ${String(plan.output.kind)}`);
  if (!Array.isArray(plan.output.node_ids) || plan.output.node_ids.length === 0 || plan.output.node_ids.some((nodeId) => typeof nodeId !== "string" || nodeId === "")) {
    throw new Error("indexed_images output plan must contain non-empty node_ids");
  }
  return splitIndexedImageHistory(history, executionId, jobId, memberIndex, plan.output.node_ids as string[]);
}
