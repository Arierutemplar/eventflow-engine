import "../tracing.js";
import type { ConsumeMessage } from "amqplib";
import {
  MAX_RETRIES,
  RabbitMQClient,
  RETRY_QUEUES,
} from "@eventflow/event-bus";
import { handleEvent } from "../handlers/event.handler.js";
import {
  executeIdempotently,
  IdempotencyConflictError,
} from "../services/idempotency.service.js";
import {
  eventsDeadLetteredTotal,
  eventsDuplicatesTotal,
  eventsErrorsTotal,
  eventsProcessedTotal,
  eventsRetriesTotal,
  startMetricsServer,
} from "../metrics.js";
import {
  extractTraceContext,
  startConsumerSpan,
} from "../tracing.js";
import { logger } from "@eventflow/common";
import { RedisIdempotencyService } from "../services/redis-idempotency.service.js";

type EventEnvelope = {
  id?: unknown;
  eventId?: unknown;
  eventType?: unknown;
  payload?: unknown;
};

type EventPayloadMetadata = {
  metadata?: {
    traceparent?: unknown;
  };
};

function getEventId(event: EventEnvelope): string {
  const eventId = event.eventId ?? event.id;
  if (typeof eventId !== "string" || eventId.trim() === "") {
    throw new Error("El evento no contiene un eventId válido");
  }
  return eventId;
}

function getHeaders(msg: ConsumeMessage): Record<string, unknown> {
  if (
    typeof msg.properties.headers !== "object" ||
    msg.properties.headers === null
  ) {
    return {};
  }
  return { ...(msg.properties.headers as Record<string, unknown>) };
}

function getRetryCount(msg: ConsumeMessage): number {
  const headers = getHeaders(msg);
  const explicitCount = headers["x-retry-count"];
  if (typeof explicitCount === "number" && Number.isInteger(explicitCount)) {
    return Math.max(0, explicitCount);
  }
  if (typeof explicitCount === "string" && /^\d+$/.test(explicitCount)) {
    return Number(explicitCount);
  }

  const deaths = headers["x-death"];
  if (!Array.isArray(deaths)) return 0;

  return deaths.reduce((total, death) => {
    if (typeof death !== "object" || death === null) return total;
    const queue = "queue" in death ? death.queue : undefined;
    const count = "count" in death ? death.count : undefined;
    if (
      typeof queue === "string" &&
      RETRY_QUEUES.some((retryQueue) => retryQueue.name === queue) &&
      typeof count === "number"
    ) {
      return total + count;
    }
    return total;
  }, 0);
}

export async function startEventConsumer(): Promise<void> {
  logger.info("[*] event-workers consumer ready");
  startMetricsServer(Number(process.env.CONSUMER_METRICS_PORT ?? 4002));

  const rabbitUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
  const rabbitMQ = RabbitMQClient.getInstance(rabbitUrl);
  await rabbitMQ.connect();
  const redisIdempotency = process.env.REDIS_URL
    ? new RedisIdempotencyService()
    : null;

  const queueName = process.env.RABBITMQ_QUEUE ?? "events_queue";
  logger.info({ queue: queueName }, "Escuchando la cola de RabbitMQ");

  await rabbitMQ.consume(queueName, async (msg) => {
    if (!msg) return;

    let eventData: EventEnvelope = {};
    let processingSpan: ReturnType<typeof startConsumerSpan> | undefined;
    let redisClaimed = false;
    let eventId: string | undefined;
    try {
      eventData = JSON.parse(msg.content.toString()) as EventEnvelope;
      eventId = getEventId(eventData);
      if (redisIdempotency) {
        const redisClaim = await redisIdempotency.tryAcquire(eventId);
        if (redisClaim === null) {
          logger.error(
            { eventId, eventType: eventData.eventType },
            "Redis no disponible para idempotencia; se continúa en modo fail-open",
          );
        } else if (!redisClaim) {
          logger.warn(
            { eventId, eventType: eventData.eventType, queue: queueName },
            "Evento duplicado detectado en Redis; se confirma sin reprocesar",
          );
          eventsDuplicatesTotal.inc({
            event_type:
              typeof eventData.eventType === "string"
                ? eventData.eventType
                : "unknown",
          });
          rabbitMQ.ack(msg);
          return;
        } else {
          redisClaimed = true;
        }
      }
      const eventPayload =
        typeof eventData.payload === "object" && eventData.payload !== null
          ? (eventData.payload as EventPayloadMetadata & { name?: unknown })
          : {};
      const parentContext = extractTraceContext(
        eventPayload.metadata?.traceparent,
      );
      processingSpan = startConsumerSpan(
        `event.process ${String(eventData.eventType ?? "unknown")}`,
        parentContext,
      );
      if (eventPayload.name === "FAIL_TEST") {
        throw new Error("Fallo de prueba DLQ solicitado por el payload");
      }

      const result = await executeIdempotently(eventId, async () => {
        logger.info({ eventId }, "Procesando evento");
        await handleEvent(eventData.payload ?? eventData);
        return { processed: true, eventId };
      });

      if (result.replayed) {
        logger.info({ eventId }, "Evento duplicado ignorado");
        eventsDuplicatesTotal.inc({
          event_type:
            typeof eventData.eventType === "string"
              ? eventData.eventType
              : "unknown",
        });
      } else {
        logger.info({ eventId }, "Evento procesado exitosamente");
        eventsProcessedTotal.inc({
          event_type:
            typeof eventData.eventType === "string"
              ? eventData.eventType
              : "unknown",
        });
      }

      await redisIdempotency?.markProcessed(eventId);
      rabbitMQ.ack(msg);
    } catch (error) {
      if (redisIdempotency && redisClaimed && eventId) {
        try {
          await redisIdempotency.release(eventId);
        } catch (releaseError: unknown) {
          logger.error(
            { err: releaseError, eventId },
            "No se pudo liberar el lock de idempotencia Redis",
          );
        }
      }
      if (error instanceof IdempotencyConflictError) {
        logger.warn(
          { err: error },
          "Evento ya está siendo procesado; se enviará a retry/DLQ",
        );
      } else {
        logger.error({ err: error }, "Error no controlado procesando el evento");
      }

      const retryCount = getRetryCount(msg);
      const failedEvent = eventData ?? {};
      const failedEventType =
        typeof failedEvent.eventType === "string"
          ? failedEvent.eventType
          : "unknown";
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      eventsErrorsTotal.inc({ event_type: failedEventType });
      const routingKey =
        typeof msg.fields.routingKey === "string"
          ? msg.fields.routingKey
          : "events";

      try {
        if (retryCount < MAX_RETRIES) {
          const nextAttempt = retryCount + 1;
          const headers = {
            ...getHeaders(msg),
            "x-retry-count": nextAttempt,
            "x-error-message": errorMessage,
          };
          const retryQueue = await rabbitMQ.publishToRetry(
            nextAttempt,
            routingKey,
            failedEvent,
            headers,
          );
          rabbitMQ.ack(msg);
          logger.warn(
            { attempt: nextAttempt, maxRetries: MAX_RETRIES, retryQueue },
            "Reintento enviado",
          );
          eventsRetriesTotal.inc({
            event_type:
              failedEventType,
            retry_queue: retryQueue,
          });
        } else {
          await rabbitMQ.publishToDeadLetter(
            routingKey,
            failedEvent,
            {
              ...getHeaders(msg),
              "x-retry-count": retryCount,
              "x-error-message": errorMessage,
            },
          );
          rabbitMQ.ack(msg);
          logger.warn(
            { maxRetries: MAX_RETRIES, queue: "DLQ" },
            "Máximo de reintentos alcanzado; evento enviado a DLQ",
          );
          eventsDeadLetteredTotal.inc({
            event_type:
              failedEventType,
          });
        }
      } catch (routingError) {
        logger.error(
          { err: routingError },
          "No se pudo enrutar el mensaje; se rechaza hacia la DLQ",
        );
        rabbitMQ.nack(msg, false, false);
      }
    } finally {
      processingSpan?.end();
    }
  });
}

startEventConsumer().catch((error: unknown) => {
  logger.fatal({ err: error }, "Error fatal iniciando el consumer");
  process.exitCode = 1;
});
