import "./tracing.js";
import Fastify from "fastify";
import type { FastifyRequest } from "fastify";
import { Prisma } from "@prisma/client";
import { database } from "@eventflow/database";
import { registerEventRoutes } from "./routes/event.routes.js";
import { registerProductRoutes } from "./products/product.router.js";
import {
  httpRequestDurationSeconds,
  httpRequestsTotal,
  metricsRegistry,
} from "./metrics.js";

const app = Fastify({ logger: true });
const requestStartTimes = new WeakMap<FastifyRequest, bigint>();

app.addHook("onRequest", async (request) => {
  requestStartTimes.set(request, process.hrtime.bigint());
});

app.addHook("onResponse", async (request, reply) => {
  const startTime = requestStartTimes.get(request);
  if (!startTime) return;

  const route = request.routeOptions.url ?? "unknown";
  const labels = {
    method: request.method,
    route,
    status_code: String(reply.statusCode),
  };
  const durationSeconds =
    Number(process.hrtime.bigint() - startTime) / 1_000_000_000;

  httpRequestsTotal.inc(labels);
  httpRequestDurationSeconds.observe(labels, durationSeconds);
});

app.get("/health", async () => ({ status: "ok" }));
app.get("/metrics", async (_request, reply) => {
  reply.header("Content-Type", metricsRegistry.contentType);
  return reply.send(await metricsRegistry.metrics());
});
await registerEventRoutes(app);
await registerProductRoutes(app, database);

app.setErrorHandler((error, request, reply) => {
  request.log.error(error);

  if (isValidationError(error) || isBadRequestError(error)) {
    return reply.status(400).send({
      success: false,
      error: {
        code: "VALIDATION_ERROR",
        message: "La petición contiene datos inválidos",
      },
    });
  }

  if (
    error instanceof Prisma.PrismaClientKnownRequestError &&
    error.code === "P2002"
  ) {
    return reply.status(409).send({
      success: false,
      error: {
        code: "CONFLICT",
        message: "El recurso ya existe",
      },
    });
  }

  return reply.status(500).send({
    success: false,
    error: {
      code: "INTERNAL_SERVER_ERROR",
      message: "Error interno del servidor",
    },
  });
});

function isValidationError(error: unknown): error is { validation: unknown } {
  return (
    typeof error === "object" &&
    error !== null &&
    "validation" in error &&
    Array.isArray(error.validation)
  );
}

function isBadRequestError(
  error: unknown,
): error is { code?: string; statusCode?: number } {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const candidate = error as { code?: unknown; statusCode?: unknown };
  return (
    (typeof candidate.statusCode === "number" &&
      candidate.statusCode === 400) ||
    candidate.code === "FST_ERR_CTP_INVALID_JSON_BODY"
  );
}

const port = Number(process.env.API_PORT ?? 3002);
await app.listen({ host: "0.0.0.0", port });