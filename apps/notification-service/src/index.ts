import type { ConsumeMessage } from "amqplib";
import { logger } from "@eventflow/common";
import {
  RabbitMQClient,
  RETRY_QUEUES,
} from "@eventflow/event-bus";
import { executeNotificationOnce } from "./idempotency.js";
import {
  notificationsDuplicatesTotal,
  notificationsErrorsTotal,
  notificationsProcessedTotal,
  notificationsRetriesTotal,
  startMetricsServer,
  stopMetricsServer,
} from "./metrics.js";

type EventEnvelope = {
  id?: unknown;
  eventId?: unknown;
  eventType?: unknown;
  payload?: unknown;
};

const queueName = "notifications_queue";
const routingPattern = "product.*";

function eventIdOf(event: EventEnvelope): string {
  const eventId = event.eventId ?? event.id;
  if (typeof eventId !== "string" || eventId.trim() === "") {
    throw new Error("El evento no contiene un eventId válido");
  }
  return eventId;
}

function headersOf(message: ConsumeMessage): Record<string, unknown> {
  const headers = message.properties.headers;
  return typeof headers === "object" && headers !== null
    ? { ...(headers as Record<string, unknown>) }
    : {};
}

function retryCountOf(message: ConsumeMessage): number {
  const deaths = headersOf(message)["x-death"];
  if (!Array.isArray(deaths)) return 0;

  return deaths.reduce((total, death) => {
    if (typeof death !== "object" || death === null) return total;
    const queue = "queue" in death ? death.queue : undefined;
    const count = "count" in death ? death.count : undefined;
    return typeof queue === "string" &&
      RETRY_QUEUES.some((retryQueue) => retryQueue.name === queue) &&
      typeof count === "number"
      ? total + count
      : total;
  }, 0);
}

function eventTypeOf(event: EventEnvelope): string {
  return typeof event.eventType === "string" ? event.eventType : "unknown";
}

async function handleNotification(event: EventEnvelope): Promise<void> {
  logger.info(
    { eventType: event.eventType },
    "Notificación generada",
  );
}

async function consumeMessage(
  rabbit: RabbitMQClient,
  message: ConsumeMessage,
): Promise<void> {
  const event = JSON.parse(message.content.toString()) as EventEnvelope;
  const eventId = eventIdOf(event);
  const result = await executeNotificationOnce(eventId, () =>
    handleNotification(event).then(() => ({ eventId })),
  );

  if (result.duplicate) {
    logger.info({ eventId }, "Evento duplicado ignorado");
    notificationsDuplicatesTotal.inc({ event_type: eventTypeOf(event) });
  } else {
    logger.info({ eventId }, "Evento procesado");
    notificationsProcessedTotal.inc({ event_type: eventTypeOf(event) });
  }
  rabbit.ack(message);
}

async function routeFailure(
  rabbit: RabbitMQClient,
  message: ConsumeMessage,
): Promise<void> {
  const retryCount = retryCountOf(message);
  const event = JSON.parse(message.content.toString()) as EventEnvelope;
  const routingKey =
    typeof message.fields.routingKey === "string"
      ? message.fields.routingKey
      : "product.event";

  if (retryCount < RETRY_QUEUES.length) {
    const retryQueue = await rabbit.publishToRetry(
      retryCount + 1,
      routingKey,
      event,
      headersOf(message),
    );
    rabbit.ack(message);
    logger.warn(
      {
        attempt: retryCount + 1,
        maxRetries: RETRY_QUEUES.length,
        retryQueue,
      },
      "Reintento de notificación enviado",
    );
    notificationsRetriesTotal.inc({
      event_type: eventTypeOf(event),
      retry_queue: retryQueue,
    });
    return;
  }

  await rabbit.publishToDeadLetter(
    routingKey,
    event,
    headersOf(message),
  );
  rabbit.ack(message);
  logger.error(
    { eventId: eventIdOf(event) },
    "Evento de notificación enviado a DLQ",
  );
}

async function main(): Promise<void> {
  const rabbit = RabbitMQClient.getInstance(
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672",
  );
  startMetricsServer();
  await rabbit.connect();
  await rabbit.consume(queueName, async (message) => {
    if (!message) return;
    try {
      await consumeMessage(rabbit, message);
    } catch (error) {
      logger.error({ err: error }, "Error procesando evento de notificación");
      const event = JSON.parse(message.content.toString()) as EventEnvelope;
      notificationsErrorsTotal.inc({ event_type: eventTypeOf(event) });
      try {
        await routeFailure(rabbit, message);
      } catch (routingError) {
        logger.error(
          { err: routingError },
          "Error enviando notificación a retry/DLQ",
        );
        rabbit.nack(message, false, false);
      }
    }
  }, routingPattern);
  logger.info(
    { queue: queueName, routingPattern },
    "Consumer de notificaciones iniciado",
  );
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Error fatal en notification-service");
  process.exitCode = 1;
});

const shutdown = async (): Promise<void> => {
  await stopMetricsServer();
  process.exit(0);
};

process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
