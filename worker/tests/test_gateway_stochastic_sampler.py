import importlib.util
import json
import pathlib
import sys
import types
import unittest


class FakeTensor:
    def __init__(self, values):
        self.values = list(values)
        self.shape = (len(self.values), 1)
        self.ndim = 2
        self.is_nested = False

    def __getitem__(self, item):
        result = self.values[item]
        return FakeTensor(result if isinstance(result, list) else [result])


def load_plugin():
    calls = []

    torch = types.ModuleType("torch")
    torch.is_tensor = lambda value: isinstance(value, FakeTensor)
    torch.cat = lambda values, dim=0: FakeTensor([item for value in values for item in value.values])

    sample = types.ModuleType("comfy.sample")
    sample.prepare_noise = lambda latent, seed: FakeTensor([f"noise:{seed}:{value}" for value in latent.values])
    comfy = types.ModuleType("comfy")
    comfy.sample = sample

    comfy_extras = types.ModuleType("comfy_extras")
    custom_sampler = types.ModuleType("comfy_extras.nodes_custom_sampler")

    class NodeOutput:
        def __init__(self, *args):
            self.result = args

    class SamplerCustomAdvanced:
        def sample(self, noise, guider, sampler, sigmas, latent_image):
            calls.append({"seed": noise.seed, "latent": latent_image})
            generated = noise.generate_noise(latent_image)
            return NodeOutput(
                {"samples": generated, "marker": "output"},
                {"samples": FakeTensor([f"denoised:{value}" for value in generated.values])},
            )

    custom_sampler.SamplerCustomAdvanced = SamplerCustomAdvanced
    original = {
        name: sys.modules.get(name)
        for name in (
            "torch",
            "comfy",
            "comfy.sample",
            "comfy_extras",
            "comfy_extras.nodes_custom_sampler",
        )
    }
    sys.modules.update({
        "torch": torch,
        "comfy": comfy,
        "comfy.sample": sample,
        "comfy_extras": comfy_extras,
        "comfy_extras.nodes_custom_sampler": custom_sampler,
    })
    try:
        module_path = pathlib.Path(__file__).parents[1] / "ComfyUI-Gateway-Batch" / "__init__.py"
        spec = importlib.util.spec_from_file_location("gateway_stochastic_sampler_test", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        for name, value in original.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    return module, calls


class GatewayMultiSeedStochasticSamplerTests(unittest.TestCase):
    def test_samples_each_member_with_its_own_seed_and_rejoins_latents(self):
        module, calls = load_plugin()
        latent = {
            "samples": FakeTensor(["first", "second"]),
            "noise_mask": FakeTensor(["mask-first", "mask-second"]),
            "batch_index": [8, 9],
        }

        output, denoised = module.GatewayMultiSeedStochasticSampler().sample(
            json.dumps([101, 202]), object(), object(), object(), latent
        )

        self.assertEqual([call["seed"] for call in calls], [101, 202])
        self.assertEqual([call["latent"]["samples"].values for call in calls], [["first"], ["second"]])
        self.assertEqual([call["latent"]["noise_mask"].values for call in calls], [["mask-first"], ["mask-second"]])
        self.assertEqual([call["latent"]["batch_index"] for call in calls], [[8], [9]])
        self.assertEqual(output["samples"].values, ["noise:101:first", "noise:202:second"])
        self.assertEqual(denoised["samples"].values, ["denoised:noise:101:first", "denoised:noise:202:second"])

    def test_rejects_seed_count_that_does_not_match_the_latent_batch(self):
        module, _ = load_plugin()
        with self.assertRaisesRegex(ValueError, "does not match"):
            module.GatewayMultiSeedStochasticSampler().sample(
                "[1]", object(), object(), object(), {"samples": FakeTensor(["first", "second"])}
            )


if __name__ == "__main__":
    unittest.main()
