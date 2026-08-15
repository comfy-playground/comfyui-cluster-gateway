# AGENTS.md

## Scope

This repository contains a TypeScript gateway that presents a subset of the
native ComfyUI HTTP API while scheduling requests across independent ComfyUI
workers. The LoRA Manager MCP is an independent Git submodule in `mcp/`; this
repository contains a small Python custom node for ComfyUI workers.

## Runtime and commands

- Use Node.js `>= 22.18.0`. The project uses the built-in `node:sqlite` API.
- Install dependencies with `npm ci`.
- Run `npm run typecheck`, `npm test`, and `npm run build` before handing off
  TypeScript changes.
- When the host Node.js version is unsuitable, run tests in the documented
  `node:22.18-alpine` container instead. Do not lower the engine requirement
  to accommodate an older host runtime.
- Initialize the MCP submodule with `git submodule update --init --recursive`.
  Run its commands from `mcp/`; do not add MCP packages to this root package.

## Code boundaries

- Gateway HTTP and scheduling code belongs in `src/`. Preserve native
  ComfyUI-compatible request and response shapes for public routes.
- `mcp/` is the independently versioned `comfyui-lora-manager-mcp` submodule.
  Keep its source, tests, Docker image, and release history there. Gateway
  changes must consume its public HTTP contract rather than import its code.
- `worker/ComfyUI-Gateway-Batch/` is the only Python surface. It is copied
  into each ComfyUI worker under `custom_nodes/`; do not introduce Python for
  gateway, tooling, or deployment changes.
- `config.yaml`, `.env`, and `data/` are local deployment state. Keep the
  gateway examples in `config.example.yaml` and `.env.example` synchronized.

## Change rules

- Configuration parsing is intentionally strict. Adding a gateway field
  requires updating `src/config.ts`, `src/types.ts`, the example config, and
  focused tests.
- SQLite migrations in `src/database.ts` must remain backward compatible with
  existing gateway databases.
- Batch adapters must preserve per-request prompt IDs, history, image
  ownership, and independent seeds. A worker lacking the required custom node
  must safely fall back to singleton execution.
- The primary worker is the only LoRA catalog control plane. Do not broaden
  `/api/lm/*` routing without considering authorization, draining, and catalog
  synchronization across every ready worker.
- Keep the Gateway Docker service non-root, read-only, capability-dropped, and
  bound to loopback by default. MCP deployment settings belong in its
  submodule, never in this Compose file.
- Do not add credentials, worker URLs, GPU names, generated output, SQLite
  files, or model data to Git.

## Tests

- `test/integration.test.ts` exercises gateway behavior through mock ComfyUI
  HTTP servers.
- `test/batching.test.ts` covers batch graph transformations.
- Run `python3 -m unittest discover -s worker/tests` when modifying the worker
  custom node. These tests mock ComfyUI and Torch imports, so no model runtime
  is needed.
