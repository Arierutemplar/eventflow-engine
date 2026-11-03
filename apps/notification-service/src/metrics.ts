import http from "node:http";
import { Counter, Registry, collectDefaultMetrics } from "prom-client";
import { logger } from "@eventflow/common";

export const metricsRegistry = new Registry();
collectDefaultMetrics({ register: metricsRegistry });

export const notificationsProcessedTotal = new Counter({
  name: "eventflow_notification_events_processed_total",
  help: "Total de eventos procesados exitosamente por notification-service.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const notificationsDuplicatesTotal = new Counter({
  name: "eventflow_notification_events_duplicates_total",
  help: "Total de eventos duplicados ignorados por notification-service.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const notificationsErrorsTotal = new Counter({
  name: "eventflow_notification_events_errors_total",
  help: "Total de errores al procesar eventos en notification-service.",
  registers: [metricsRegistry],
  labelNames: ["event_type"],
});

export const notificationsRetriesTotal = new Counter({
  name: "eventflow_notification_events_retries_total",
  help: "Total de eventos enviados a reintento por notification-service.",
  registers: [metricsRegistry],
  labelNames: ["event_type", "retry_queue"],
});

let server: http.Server | null = null;

export function startMetricsServer(
  port = Number(process.env.METRICS_PORT ?? 4003),
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
    logger.info(
      { port },
      "Servidor de métricas de notificaciones iniciado",
    );
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
