import http from "node:http";
import {
  Counter,
  Gauge,
  Registry,
  collectDefaultMetrics,
} from "prom-client";
import { logger } from "@eventflow/common";

export const metricsRegistry = new Registry();
collectDefaultMetrics({ register: metricsRegistry });

export const outboxPublishedTotal = new Counter({
  name: "eventflow_outbox_messages_published_total",
  help: "Total de mensajes Outbox publicados en RabbitMQ.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const outboxPublishFailuresTotal = new Counter({
  name: "eventflow_outbox_publish_failures_total",
  help: "Total de fallos al publicar mensajes Outbox.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const eventsProcessedTotal = new Counter({
  name: "eventflow_events_processed_total",
  help: "Total de eventos procesados exitosamente.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const eventsDuplicatesTotal = new Counter({
  name: "eventflow_events_duplicates_total",
  help: "Total de eventos duplicados ignorados por idempotencia.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const eventsErrorsTotal = new Counter({
  name: "eventflow_events_errors_total",
  help: "Total de errores al procesar eventos.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const eventsRetriesTotal = new Counter({
  name: "eventflow_events_retries_total",
  help: "Total de eventos enviados a una cola de reintento.",
  registers: [metricsRegistry],
  labelNames: ["event_type", "retry_queue"],
});

export const eventsDeadLetteredTotal = new Counter({
  name: "eventflow_events_dead_lettered_total",
  help: "Total de eventos enviados a la DLQ.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const outboxPending = new Gauge({
  name: "eventflow_outbox_messages_pending",
  help: "Mensajes Outbox pendientes en el último polling.",
  registers: [metricsRegistry],
});

let server: http.Server | null = null;

export function startMetricsServer(
  port = Number(process.env.METRICS_PORT ?? 4001),
): void {
  if (server) return;

  server = http.createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("Not found");
      return;
    }

    response.statusCode = 200;
    response.setHeader("Content-Type", metricsRegistry.contentType);
    response.end(await metricsRegistry.metrics());
  });

  server.listen(port, "0.0.0.0", () => {
    logger.info({ port }, "Servidor de métricas Prometheus iniciado");
  });
}

export async function stopMetricsServer(): Promise<void> {
  if (!server) return;
  const activeServer = server;
  server = null;
  await new Promise<void>((resolve, reject) => {
    activeServer.close((error) => (error ? reject(error) : resolve()));
  });
}
