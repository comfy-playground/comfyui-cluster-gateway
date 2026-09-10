# ComfyUI Gateway worker nodes

This directory is copied into each ComfyUI worker image as
`custom_nodes/ComfyUI-Gateway-Batch`.

`GatewayMultiSeedNoise` provides independent initial noise for deterministic
samplers such as Euler. `GatewayMultiSeedStochasticSampler` preserves the
`SamplerCustomAdvanced` guider/sampler/sigmas/latent contract while invoking
the ComfyUI sampler once per member, so stochastic samplers such as Anima
`er_sde` receive independent seeds. It returns a merged latent on output 0;
downstream `VAEDecode` and `SaveImage` nodes do not need to change IDs.

The gateway probes `GatewayMultiSeedStochasticSampler` before enabling the
`anima-er-sde` adapter. A worker without this node safely remains singleton
only for `er_sde` workflows.

The node also optionally exposes `/gateway-worker/v1/runtime` and
`/gateway-worker/v1/memory-operations`. These worker-local endpoints provide
operation-ID based GPU offload/release status; gateways must probe for them
before using memory control.
