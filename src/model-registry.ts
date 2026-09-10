import type { JsonObject, ModelConfig, PromptEnvelope, WorkerConfig } from "./types.js";

export interface ModelRequirement {
  modelId: string;
  family: string;
  diffusion?: string;
  textEncoder?: string;
  vae?: string;
}

function nodeEntries(envelope: PromptEnvelope): Array<[string, JsonObject]> {
  return Object.entries(envelope.prompt).flatMap(([id, value]) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
    return [[id, value as JsonObject]];
  });
}

function input(node: JsonObject, key: string): string | undefined {
  const inputs = node.inputs;
  if (inputs === null || typeof inputs !== "object" || Array.isArray(inputs)) return undefined;
  const value = (inputs as JsonObject)[key];
  return typeof value === "string" ? value : undefined;
}

export function workflowFiles(envelope: PromptEnvelope): { diffusion?: string; textEncoder?: string; vae?: string } {
  let diffusion: string | undefined;
  let textEncoder: string | undefined;
  let vae: string | undefined;
  for (const [, node] of nodeEntries(envelope)) {
    if (node.class_type === "UNETLoader") diffusion ??= input(node, "unet_name");
    if (node.class_type === "CheckpointLoaderSimple") diffusion ??= input(node, "ckpt_name");
    if (node.class_type === "CLIPLoader") textEncoder ??= input(node, "clip_name");
    if (node.class_type === "VAELoader") vae ??= input(node, "vae_name");
  }
  return { ...(diffusion === undefined ? {} : { diffusion }), ...(textEncoder === undefined ? {} : { textEncoder }), ...(vae === undefined ? {} : { vae }) };
}

function sameFile(actual: string | undefined, expected: string | undefined): boolean {
  return expected === undefined || actual === expected;
}

export function requirementFor(envelope: PromptEnvelope, models: readonly ModelConfig[] = []): ModelRequirement | { error: string } {
  const files = workflowFiles(envelope);
  const requested = envelope.extra_data && typeof envelope.extra_data === "object" && !Array.isArray(envelope.extra_data)
    ? (envelope.extra_data as JsonObject).gateway
    : undefined;
  const requestedId = requested && typeof requested === "object" && !Array.isArray(requested)
    ? (requested as JsonObject).model_id : undefined;
  if (requestedId !== undefined && typeof requestedId !== "string") return { error: "gateway.model_id must be a string" };
  if (models.length === 0) {
    if (requestedId) return { modelId: requestedId, family: requestedId };
    return { modelId: "legacy", family: "legacy", ...files };
  }
  const candidates = requestedId ? models.filter((model) => model.id === requestedId) : models.filter((model) =>
    sameFile(files.diffusion, model.diffusion) && sameFile(files.textEncoder, model.textEncoder) && sameFile(files.vae, model.vae));
  if (candidates.length === 0 && !requestedId) return { modelId: "legacy", family: "legacy", ...files };
  if (candidates.length !== 1) return { error: requestedId ? `unknown model_id ${requestedId}` : "workflow model dependencies match multiple registered models" };
  const model = candidates[0]!;
  if (!sameFile(files.diffusion, model.diffusion) || !sameFile(files.textEncoder, model.textEncoder) || !sameFile(files.vae, model.vae)) {
    return { error: `workflow dependencies do not match model_id ${model.id}` };
  }
  return { modelId: model.id, family: model.family, diffusion: model.diffusion, ...(model.textEncoder === undefined ? {} : { textEncoder: model.textEncoder }), ...(model.vae === undefined ? {} : { vae: model.vae }) };
}

export function workerSupportsModel(worker: WorkerConfig, requirement: ModelRequirement, models: readonly ModelConfig[] = []): boolean {
  if (requirement.modelId === "legacy") return !worker.modelIds || worker.modelIds.length === 0 || worker.modelIds.includes("legacy");
  if (!worker.modelIds?.includes(requirement.modelId)) return false;
  const model = models.find((candidate) => candidate.id === requirement.modelId);
  return model !== undefined && model.capabilities.every((capability) => worker.capabilities.includes(capability));
}
