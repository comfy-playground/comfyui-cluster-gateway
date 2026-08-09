import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { loadConfig } from "./config.js";
import { GatewayDatabase } from "./database.js";
import { GatewayService } from "./gateway.js";
import { buildServer } from "./server.js";

async function main(): Promise<void> {
  const major = Number(process.versions.node.split(".")[0]);
  if (!Number.isFinite(major) || major < 22) throw new Error(`Node.js >=22 is required; current version is ${process.versions.node}`);
  const config = await loadConfig();
  await mkdir(dirname(config.databasePath), { recursive: true, mode: 0o750 });
  await mkdir(config.outputDirectory, { recursive: true, mode: 0o750 });
  const database = new GatewayDatabase(config.databasePath);
  const gateway = new GatewayService(config, database);
  await gateway.start();
  const server = await buildServer(gateway, config);
  let stopping = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    server.log.info({ signal }, "gateway shutting down");
    await server.close();
    await gateway.stop();
    database.close();
  };
  process.once("SIGINT", () => { void shutdown("SIGINT"); });
  process.once("SIGTERM", () => { void shutdown("SIGTERM"); });
  await server.listen({ host: config.listen.host, port: config.listen.port });
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
