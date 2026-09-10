import { describe, expect, it } from "vitest";
import { requirementFor, workerSupportsModel, workflowFiles } from "../src/model-registry.js";
import type { ModelConfig, PromptEnvelope, WorkerConfig } from "../src/types.js";

const model: ModelConfig = {
  id: "moody-krea-v7-fp8", family: "krea2",
  diffusion: "Moody-Krea-Mix-v7_00002__clean_fp8.safetensors",
  textEncoder: "qwen3vl_4b_fp8_scaled.safetensors", vae: "qwen_image_vae.safetensors",
  capabilities: ["default"], maxBatchSize: 1,
};
const worker: WorkerConfig = {
  id: "3090", url: "http://worker", expectedDeviceName: "gpu", secondsPerImage: 1,
  capabilities: ["default"], required: false, primary: false, enabled: true,
  modelIds: [model.id],
};
function prompt(): PromptEnvelope { return { prompt: {
  "1": { class_type: "UNETLoader", inputs: { unet_name: model.diffusion } },
  "2": { class_type: "CLIPLoader", inputs: { clip_name: model.textEncoder!, type: "krea2" } },
  "3": { class_type: "VAELoader", inputs: { vae_name: model.vae! } },
} }; }

describe("model dependency registry", () => {
  it("identifies Krea 2 from its complete workflow dependency set", () => {
    expect(workflowFiles(prompt())).toEqual({ diffusion: model.diffusion, textEncoder: model.textEncoder, vae: model.vae });
    expect(requirementFor(prompt(), [model])).toMatchObject({ modelId: model.id, family: "krea2" });
  });
  it("rejects an explicit model id whose loader files do not match", () => {
    const result = requirementFor({ ...prompt(), extra_data: { gateway: { model_id: model.id } } }, [{ ...model, diffusion: "other.safetensors" }]);
    expect(result).toMatchObject({ error: expect.stringContaining("dependencies") });
  });
  it("only permits a model on a worker explicitly assigned to it", () => {
    const requirement = requirementFor(prompt(), [model]);
    expect("error" in requirement ? false : workerSupportsModel(worker, requirement, [model])).toBe(true);
    expect("error" in requirement ? false : workerSupportsModel({ ...worker, modelIds: [] }, requirement, [model])).toBe(false);
  });

});
