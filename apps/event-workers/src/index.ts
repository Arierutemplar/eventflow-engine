import "./tracing.js";
import { RabbitMQClient } from "@eventflow/event-bus";
import { OutboxPoller } from "./outbox-poller.js";
import { startMetricsServer, stopMetricsServer } from "./metrics.js";
import { logger } from "@eventflow/common";

async function bootstrap(): Promise<void> {
  const rabbitUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
  const rabbitClient = RabbitMQClient.getInstance(rabbitUrl);
  startMetricsServer();

  logger.info("Conectando a RabbitMQ");
  await rabbitClient.connect();

  const poller = new OutboxPoller(rabbitClient, {
    intervalMs: 5_000,
    batchSize: 10,
  });
  poller.start();

  const shutdown = async (): Promise<void> => {
    logger.info("Apagando workers de forma segura");
    poller.stop();
    await rabbitClient.close();
    await stopMetricsServer();
    process.exit(0);
  };

  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());
}

bootstrap().catch((error: unknown) => {
  logger.fatal({ err: error }, "Error fatal iniciando los workers");
  process.exit(1);
});
