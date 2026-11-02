import { prisma } from "@eventflow/database";
import { RabbitMQClient } from "@eventflow/event-bus";
import {
  executeIdempotently,
  IdempotencyConflictError,
} from "./services/idempotency.service.js";
import {
  outboxPending,
  outboxPublishFailuresTotal,
  outboxPublishedTotal,
} from "./metrics.js";
import { logger } from "@eventflow/common";

export type OutboxProcessorOptions = {
  intervalMs?: number;
  batchSize?: number;
};

export class OutboxProcessor {
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private isRunning = false;
  private isProcessing = false;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly rabbitClient: RabbitMQClient,
    options: OutboxProcessorOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? 5_000;
    this.batchSize = options.batchSize ?? 10;

    if (this.intervalMs <= 0 || this.batchSize <= 0) {
      throw new Error("intervalMs y batchSize deben ser mayores que cero");
    }
  }

  start(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    logger.info(
      { intervalMs: this.intervalMs, batchSize: this.batchSize },
      "OutboxProcessor iniciado",
    );

    void this.processPendingMessages();
    this.timer = setInterval(() => {
      void this.processPendingMessages();
    }, this.intervalMs);
  }

  stop(): void {
    if (!this.isRunning) return;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.isRunning = false;
    logger.info("OutboxProcessor detenido");
  }

  async processPendingMessages(): Promise<void> {
    if (this.isProcessing) return;

    this.isProcessing = true;
    try {
      const pendingMessages = await prisma.outboxMessage.findMany({
        where: { published: false },
        take: this.batchSize,
        orderBy: { createdAt: "asc" },
      });
      outboxPending.set(pendingMessages.length);

      if (pendingMessages.length === 0) return;
      logger.info(
        { messageCount: pendingMessages.length },
        "Procesando mensajes Outbox",
      );

      for (const message of pendingMessages) {
        await this.processMessage(message);
      }
    } catch (error) {
      logger.error(
        { err: error },
        "Error consultando mensajes pendientes del Outbox",
      );
    } finally {
      this.isProcessing = false;
    }
  }

  private async processMessage(message: {
    id: string;
    eventType: string;
    payload: unknown;
    createdAt: Date;
  }): Promise<void> {
    try {
      const idempotencyKey = `outbox:${message.id}`;
      const result = await executeIdempotently(idempotencyKey, async () => {
        await this.rabbitClient.publishEvent(message.eventType, {
          id: message.id,
          eventType: message.eventType,
          payload: message.payload,
          createdAt: message.createdAt,
        });

        return { published: true };
      });

      await prisma.outboxMessage.updateMany({
        where: { id: message.id, published: false },
        data: { published: true, sentAt: new Date() },
      });

      const action = result.replayed ? "omitido; ya estaba completado" : "publicado";
      logger.info(
        { action, eventId: message.id, eventType: message.eventType },
        "Outbox procesado",
      );
      if (!result.replayed) {
        outboxPublishedTotal.inc({ event_type: message.eventType });
      }
    } catch (error) {
      if (error instanceof IdempotencyConflictError) {
        logger.warn(
          { eventId: message.id },
          "Mensaje ya está siendo procesado; se reintentará",
        );
        return;
      }

      logger.error(
        { err: error, eventId: message.id },
        "Falló el mensaje Outbox; se reintentará",
      );
      outboxPublishFailuresTotal.inc({ event_type: message.eventType });
    }
  }
}

export { OutboxProcessor as OutboxPoller };
