import { timingSafeEqual } from "node:crypto";
import websocket from "@fastify/websocket";
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { GatewayRequestError, type GatewayService, type ManagerResult } from "./gateway.js";
import type { GatewayConfig, JobRecord } from "./types.js";
import { readResponseBytes } from "./worker-client.js";

function bearer(header: string | undefined, expected: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  const provided = Buffer.from(header.slice(7));
  const wanted = Buffer.from(expected);
  return provided.length === wanted.length && timingSafeEqual(provided, wanted);
}

function errorBody(code: string, message: string, operationId = ""): Record<string, unknown> {
  const error: Record<string, unknown> = { code, message };
  if (operationId) error.operation_id = operationId;
  return { error };
}

function bodyBytes(value: unknown): Uint8Array {
  if (value === undefined || value === null) return new Uint8Array();
  if (Buffer.isBuffer(value)) return value;
  if (typeof value === "string") return Buffer.from(value);
  return Buffer.from(JSON.stringify(value));
}

function encodeQueueJob(job: JobRecord): unknown[] {
  return [job.number, job.id, {}, { gateway_status: job.status, gateway_worker: job.workerId }];
}

function managementPath(path: string): boolean {
  return path.startsWith("/api/lm/") || path.startsWith("/gateway/v1/catalog/");
}

function readAllowed(path: string): boolean {
  if (["/api/lm/loras/list", "/api/lm/loras/excluded", "/api/lm/loras/metadata", "/api/lm/loras/roots", "/api/lm/downloads/queue", "/api/lm/downloads/history", "/api/lm/downloads/history/delete"].includes(path)) return true;
  return /^\/api\/lm\/download-progress\/[^/]+$/.test(path);
}

function mutationAllowed(method: string, path: string): boolean {
  return (method === "GET" && path === "/api/lm/loras/scan") ||
    (method === "POST" && ["/api/lm/loras/exclude", "/api/lm/loras/unexclude"].includes(path));
}

export async function buildServer(gateway: GatewayService, config: GatewayConfig): Promise<FastifyInstance> {
  const app = Fastify({ logger: process.env.NODE_ENV !== "test", bodyLimit: config.limits.maxPromptBytes, requestTimeout: 0 });
  await app.register(websocket, { options: { maxPayload: 64 * 1024 } });
  const sockets = new Set<{ readonly OPEN: number; readonly readyState: number; send(data: string): void }>();

  app.addHook("onRequest", async (request, reply) => {
    const path = request.url.split("?", 1)[0] ?? request.url;
    if (managementPath(path)) {
      if (config.auth.managementToken && !bearer(request.headers.authorization, config.auth.managementToken)) {
        await reply.code(401).send(errorBody("unauthorized", "valid management bearer token required"));
      }
      return;
    }
    if (path === "/healthz" || !config.auth.generationToken) return;
    if (!bearer(request.headers.authorization, config.auth.generationToken)) {
      await reply.code(401).send(errorBody("unauthorized", "valid generation bearer token required"));
    }
  });

  app.setErrorHandler(async (error, _request, reply) => {
    if (error instanceof GatewayRequestError) {
      await reply.code(error.statusCode).send(errorBody(error.code, error.message, error.operationId));
      return;
    }
    app.log.error(error);
    await reply.code(500).send(errorBody("internal_error", "gateway request failed"));
  });

  app.get("/healthz", async () => ({ ok: true }));
  app.get("/readyz", async (_request, reply) => {
    const error = gateway.readyError();
    return error ? reply.code(503).send(errorBody("worker_not_ready", error)) : { ready: true };
  });

  app.post("/prompt", async (request) => {
    const idempotencyKey = typeof request.headers["x-idempotency-key"] === "string" ? request.headers["x-idempotency-key"].trim() : "";
    const job = await gateway.enqueue(request.body, idempotencyKey);
    return { prompt_id: job.id, number: job.number, node_errors: {} };
  });

  app.get("/history/:id", async (request) => {
    const id = (request.params as { id: string }).id;
    const history = gateway.getJob(id)?.historyJson;
    return history ? JSON.parse(history) as unknown : {};
  });

  app.get("/queue", async () => {
    const snapshot = gateway.queue();
    return { queue_running: snapshot.running.map(encodeQueueJob), queue_pending: snapshot.pending.map(encodeQueueJob) };
  });

  app.post("/queue", async (request) => {
    const body = request.body as { clear?: boolean; delete?: string[] } | undefined;
    const ids = body?.clear ? gateway.queue().pending.map((job) => job.id) : Array.isArray(body?.delete) ? body.delete : [];
    const results: Record<string, string> = {};
    for (const id of ids) {
      try { results[id] = (await gateway.cancel(id)).state; }
      catch (error) { results[id] = error instanceof Error ? error.message : String(error); }
    }
    return { results };
  });

  app.post("/interrupt", async (request) => {
    const body = request.body as { prompt_id?: unknown } | undefined;
    if (typeof body?.prompt_id !== "string" || body.prompt_id === "") throw new GatewayRequestError(400, "prompt_id_required", "prompt_id is required in multi-worker mode");
    return gateway.cancel(body.prompt_id);
  });

  app.post("/gateway/v1/jobs/:id/cancel", async (request) => gateway.cancel((request.params as { id: string }).id));

  app.get("/view", async (request, reply) => {
    const query = request.query as { filename?: string; subfolder?: string; type?: string };
    const output = gateway.openOutput(query.filename ?? "", query.subfolder ?? "", query.type ?? "output");
    if (!output) return reply.code(404).send(errorBody("output_not_found", "gateway output was not found"));
    reply.header("content-type", output.contentType).header("content-length", output.size).header("cache-control", "private, max-age=86400, immutable");
    return reply.send(output.stream);
  });

  app.get("/system_stats", async () => gateway.systemStats());
  app.get("/gateway/v1/workers", async () => ({ workers: gateway.workerSnapshots(), catalog_revision: gateway.catalogRevision() }));
  app.get("/gateway/v1/status", async () => gateway.clusterStatus());
  app.get("/gateway/v1/jobs/:id", async (request, reply) => {
    const job = gateway.getJob((request.params as { id: string }).id);
    if (!job) return reply.code(404).send(errorBody("job_not_found", "gateway job was not found"));
    return job;
  });
  app.get("/ws", { websocket: true }, (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.send(JSON.stringify({ type: "status", data: { status: { exec_info: { queue_remaining: gateway.queue().pending.length } } } }));
  });
  gateway.on("job", (event: Record<string, unknown>) => {
    const data = { prompt_id: event.jobId, worker_id: event.workerId };
    const payload = JSON.stringify({ type: event.type, data });
    for (const socket of sockets) if (socket.readyState === socket.OPEN) socket.send(payload);
  });
  gateway.on("warning", (event: Record<string, unknown>) => app.log.warn(event));

  app.all("/api/lm/*", async (request, reply) => handleManager(request, reply, gateway));
  return app;
}

async function handleManager(request: FastifyRequest, reply: FastifyReply, gateway: GatewayService): Promise<unknown> {
  const path = request.url.split("?", 1)[0] ?? request.url;
  const method = request.method;
  if (["/api/lm/pause-download", "/api/lm/resume-download", "/api/lm/cancel-download-get"].includes(path)) {
    return reply.code(409).send(errorBody("download_control_unavailable", "the configured LoRA Manager cannot safely control synchronous gateway downloads"));
  }
  if (method === "GET" && readAllowed(path)) {
    const response = await gateway.primary.client.proxy(request.url, method, undefined, undefined, false);
    let body: Uint8Array;
    try { body = await readResponseBytes(response, 16 * 1024 * 1024); }
    catch { return reply.code(502).send(errorBody("control_response_too_large", "LoRA control response exceeds 16 MiB")); }
    reply.code(response.status).header("content-type", response.headers.get("content-type") ?? "application/json")
      .header("cache-control", "no-store").header("x-comfy-gateway-catalog-revision", gateway.catalogRevision());
    return reply.send(Buffer.from(body));
  }
  if (!mutationAllowed(method, path)) return reply.code(403).send(errorBody("manager_route_not_allowed", "management method and path are not allowlisted"));
  const bytes = bodyBytes(request.body);
  const managerResult = await gateway.runCatalogMutation(request.url, method, bytes, request.headers["content-type"]);
  return sendManagerResult(reply, managerResult);
}

function sendManagerResult(reply: FastifyReply, result: ManagerResult): unknown {
  reply.code(result.status).header("content-type", result.contentType)
    .header("x-comfy-gateway-catalog-revision", result.revision)
    .header("x-comfy-gateway-catalog-sync", "ready");
  if (result.optionalUnavailable.length > 0) reply.header("x-comfy-gateway-optional-unavailable-workers", result.optionalUnavailable.join(","));
  return reply.send(Buffer.from(result.body));
}
