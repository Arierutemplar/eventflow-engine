import autocannon from "autocannon";
import type { Request } from "autocannon";
import { randomUUID } from "node:crypto";

const targetUrl =
  process.env.TARGET_URL ??
  `http://localhost:${process.env.PORT ?? "3002"}/api/users`;
const connections = parsePositiveInteger("CONNECTIONS", 50);
const duration = parsePositiveInteger("DURATION", 30);

type CreateUserPayload = {
  email: string;
  name: string;
};

function parsePositiveInteger(name: string, defaultValue: number): number {
  const value = Number(process.env[name] ?? defaultValue);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} debe ser un entero mayor que cero`);
  }
  return value;
}

function createUserPayload(): string {
  const id = randomUUID();
  const payload: CreateUserPayload = {
    email: `user-${Date.now()}-${Math.random().toString(36).slice(2)}@example.com`,
    name: `Stress User ${id}`,
  };
  return JSON.stringify(payload);
}

function setupRequest(request: Request): Request {
  request.method = "POST";
  request.headers = {
    ...request.headers,
    "content-type": "application/json",
  };
  request.body = createUserPayload();
  return request;
}

async function main(): Promise<void> {
  console.log(
    `[stress-test] POST ${targetUrl} | ${connections} conexiones | ${duration}s`,
  );

  const result = await new Promise<autocannon.Result>((resolve, reject) => {
    const instance = autocannon(
      {
        url: targetUrl,
        connections,
        duration,
        requests: [{ method: "POST", setupRequest }],
      },
      (error, completedResult) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(completedResult);
      },
    );

    autocannon.track(instance, { renderProgressBar: true });
  });

  console.log("\n=== Stress Test Summary ===");
  console.log(`Target: ${targetUrl}`);
  console.log(`Throughput: ${result.requests.average.toFixed(2)} req/sec`);
  console.log(`Latencia promedio: ${result.latency.average.toFixed(2)} ms`);
  console.log(`Latencia p99: ${result.latency.p99.toFixed(2)} ms`);
  console.log(
    `Throughput de respuesta: ${(result.throughput.average / 1024).toFixed(2)} KiB/sec`,
  );
  console.log(`Respuestas non-2xx: ${result.non2xx}`);
  console.log(`Errores de conexión: ${result.errors}`);
}

main().catch((error: unknown) => {
  console.error("[stress-test] Error ejecutando la prueba:", error);
  process.exitCode = 1;
});
