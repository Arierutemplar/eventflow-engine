import http from "node:http";
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "prom-client";
import type { CircuitBreaker } from "@eventflow/common";

export const metricsRegistry = new Registry();
collectDefaultMetrics({ register: metricsRegistry });

export const httpRequestsTotal = new Counter({
  name: "http_requests_total",
  help: "Total de peticiones HTTP recibidas por el API Gateway.",
  labelNames: ["method", "route", "status_code"],
  registers: [metricsRegistry],
});

export const httpRequestDurationSeconds = new Histogram({
  name: "http_request_duration_seconds",
  help: "Duración de las peticiones HTTP del API Gateway en segundos.",
  labelNames: ["method", "route", "status_code"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  registers: [metricsRegistry],
});

export async function metricsContent(): Promise<string> {
  return metricsRegistry.metrics();
}

export function registerCircuitBreakerMetrics(
  breaker: CircuitBreaker,
): () => void {
  const state = new Gauge({
    name: "eventflow_circuit_breaker_state",
    help: "Estado actual del circuit breaker (1 activo).",
    labelNames: ["service", "state"],
    registers: [metricsRegistry],
  });

  return () => {
    const current = breaker.currentState;
    for (const value of ["closed", "open", "half-open"]) {
      state.set(
        { service: breaker.name, state: value },
        current === value ? 1 : 0,
      );
    }
  };
}

let server: http.Server | null = null;

export function startMetricsServer(
  collect: () => void,
  port = Number(process.env.GATEWAY_METRICS_PORT ?? 4004),
): void {
  if (server) return;
  server = http.createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("Not found");
      return;
    }
    collect();
    response.statusCode = 200;
    response.setHeader("Content-Type", metricsRegistry.contentType);
    response.end(await metricsContent());
  });
  server.listen(port, "0.0.0.0", () => {
    console.log(`[metrics] Gateway Prometheus en http://0.0.0.0:${port}/metrics`);
  });
}
