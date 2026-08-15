import { describe, expect, it } from "vitest";
import { createBatchAdapters } from "../src/batching/adapters.js";
import { BatchAdapterRegistry } from "../src/batching/registry.js";
import type { JsonObject } from "../src/types.js";

function animaPrompt(seed: number, samplerName = "euler", scheduler = "sgm_uniform"): JsonObject {
  return {
    "1": { class_type: "UNETLoader", inputs: { unet_name: "anima-base-v1.0.safetensors", weight_dtype: "default" } },
    "2": { class_type: "CLIPLoader", inputs: { clip_name: "qwen_3_06b_base.safetensors", type: "stable_diffusion", device: "default" } },
    "3": { class_type: "VAELoader", inputs: { vae_name: "qwen_image_vae.safetensors" } },
    "5": { class_type: "CLIPTextEncode", inputs: { text: "same", clip: ["2", 0] } },
    "6": { class_type: "CLIPTextEncode", inputs: { text: "bad", clip: ["2", 0] } },
    "7": { class_type: "EmptyLatentImage", inputs: { width: 1024, height: 1024, batch_size: 1 } },
    "13": { class_type: "KSampler", inputs: { seed, steps: 35, cfg: 4.5, sampler_name: samplerName, scheduler, denoise: 1, model: ["1", 0], positive: ["5", 0], negative: ["6", 0], latent_image: ["7", 0] } },
    "14": { class_type: "VAEDecode", inputs: { samples: ["13", 0], vae: ["3", 0] } },
    "15": { class_type: "SaveImage", inputs: { filename_prefix: "anima", images: ["14", 0] } },
  };
}

describe("batch adapter registry", () => {
  const registry = new BatchAdapterRegistry(createBatchAdapters());

  it("recognizes Anima euler/sgm_uniform graphs and preserves their dialect", () => {
    const result = registry.assess({ prompt: animaPrompt(123) });
    expect(result.candidate).toMatchObject({ adapterId: "anima-euler-sgm", seed: "123" });
    const adapter = registry.get("anima-euler-sgm")!;
    const physical = adapter.merge([result.candidate!, { ...result.candidate!, seed: "456" }], "00000000-0000-0000-0000-000000000001");
    const prompt = physical.envelope.prompt as JsonObject;
    expect(prompt["7"]).toMatchObject({ inputs: { batch_size: 2 } });
    expect(prompt["13"]).toMatchObject({ class_type: "SamplerCustomAdvanced" });
    expect(prompt["18"]).toMatchObject({ class_type: "KSamplerSelect", inputs: { sampler_name: "euler" } });
    expect(prompt["19"]).toMatchObject({ class_type: "BasicScheduler", inputs: { scheduler: "sgm_uniform" } });
  });

  it("builds Anima er_sde batches without changing prompt links or output node IDs", () => {
    const result = registry.assess({ prompt: animaPrompt(123, "er_sde") });
    expect(result.candidate).toMatchObject({ adapterId: "anima-er-sde", seed: "123" });
    const adapter = registry.get("anima-er-sde")!;
    const physical = adapter.merge([result.candidate!, { ...result.candidate!, seed: "456" }], "00000000-0000-0000-0000-000000000001");
    const prompt = physical.envelope.prompt as JsonObject;
    expect(prompt["13"]).toMatchObject({ class_type: "GatewayMultiSeedStochasticSampler" });
    expect(prompt["15"]).toMatchObject({ class_type: "SaveImage", inputs: { images: ["14", 0] } });
    const guider = Object.values(prompt).find((value) => value && typeof value === "object" && !Array.isArray(value) && value.class_type === "CFGGuider") as JsonObject;
    expect(guider.inputs).toMatchObject({ positive: ["5", 0], negative: ["6", 0] });
    expect((prompt["13"] as JsonObject).inputs).toMatchObject({ seeds: "[\"123\",\"456\"]" });
  });

  it("splits only the output nodes declared by the physical batch plan", () => {
    const adapter = registry.get("anima-euler-sgm")!;
    const assessment = adapter.assess({ prompt: animaPrompt(123) });
    expect(assessment.kind).toBe("candidate");
    if (assessment.kind !== "candidate") return;
    const physical = adapter.merge([assessment.candidate, { ...assessment.candidate, seed: "456" }], "00000000-0000-0000-0000-000000000001");
    const executionId = "00000000-0000-0000-0000-000000000001";
    const memberId = "00000000-0000-0000-0000-000000000002";
    const history: JsonObject = {
      [executionId]: {
        status: { status_str: "success", completed: true },
        outputs: {
          "15": { images: [{ filename: "first.png" }, { filename: "second.png" }] },
          "99": { images: [{ filename: "ignored-a.png" }, { filename: "ignored-b.png" }] },
        },
      },
    };
    const member = adapter.splitHistory(history, physical.plan, executionId, memberId, 1);
    expect(member[memberId]).toMatchObject({ outputs: { "15": { images: [{ filename: "second.png" }] } } });
    expect((member[memberId] as JsonObject).outputs).not.toHaveProperty("99");
  });
});
