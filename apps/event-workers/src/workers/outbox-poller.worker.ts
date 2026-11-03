import { Prisma, type PrismaClient } from "@prisma/client";
import type { Channel } from "amqplib";
import { EVENT_EXCHANGE } from "@eventflow/event-bus";
import { logger } from "@eventflow/common";

export type OutboxPollerWorkerOptions = {
  intervalMs?: number;
  batchSize?: number;
  exchange?: string;
};

type OutboxEvent = {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: Date;
};

export class OutboxPollerWorker {
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private readonly exchange: string;
  private timer: ReturnType<typeof setInterval> | null = null;
  private isRunning = false;
  private isPolling = false;
  private pollingPromise: Promise<void> | null = null;

  constructor(
    private readonly prisma: PrismaClient,
    private readonly channel: Channel,
    options: OutboxPollerWorkerOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? 5_000;
    this.batchSize = options.batchSize ?? 50;
    this.exchange = options.exchange ?? EVENT_EXCHANGE;

    if (this.intervalMs <= 0 || this.batchSize <= 0) {
      throw new Error("intervalMs y batchSize deben ser mayores que cero");
    }
  }

  start(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    logger.info(
      { intervalMs: this.intervalMs, batchSize: this.batchSize },
      "Outbox worker iniciado",
    );

    void this.poll();
    this.timer = setInterval(() => {
      void this.poll();
    }, this.intervalMs);
  }

  async stop(): Promise<void> {
    if (!this.isRunning && !this.pollingPromise) return;

    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    if (this.pollingPromise) {
      await this.pollingPromise;
    }
    logger.info("Outbox worker detenido");
  }

  async poll(): Promise<void> {
    if (this.isPolling) return;

    this.isPolling = true;
    this.pollingPromise = this.processBatch();
    try {
      await this.pollingPromise;
    } finally {
      this.pollingPromise = null;
      this.isPolling = false;
    }
  }

  private async processBatch(): Promise<void> {
  try {
    await this.prisma.$transaction(async (transaction) => {
      const events = await transaction.$queryRaw<OutboxEvent[]>(
        Prisma.sql`
          SELECT
            "id",
            "aggregateId",
            "eventType",
            "payload",
            "createdAt"
          FROM "OutboxEvent"
          WHERE "status" = 'PENDING'
          ORDER BY "createdAt" ASC
          LIMIT ${this.batchSize}
          FOR UPDATE SKIP LOCKED
        `,
      );

      if (events.length === 0) return;
      logger.info(
        { eventCount: events.length },
        "Procesando eventos Outbox pendientes",
      );

      for (const event of events) {
        await this.processEvent(transaction, event);
      }
    });
  } catch (error: unknown) {
    logger.error({ err: error }, "Error consultando eventos pendientes");
  }
  }

  private async processEvent(
  transaction: Prisma.TransactionClient,
  event: OutboxEvent,
  ): Promise<void> {
  try {
    await this.publishEvent(event);
    const updated = await transaction.outboxEvent.updateMany({
      where: {
        id: event.id,
        status: "PENDING",
      },
      data: { status: "PUBLISHED" },
    });

    if (updated.count !== 1) {
      logger.warn(
        { eventId: event.id },
        "El evento fue publicado, pero no se pudo marcar como PUBLISHED",
      );
      return;
    }

    logger.info(
      { eventId: event.id, eventType: event.eventType },
      "Evento Outbox publicado",
    );
  } catch (error: unknown) {
    logger.error(
      { err: error, eventId: event.id },
      "Error publicando el evento; se reintentará en el siguiente ciclo",
    );
  }
  }

  private async publishEvent(event: OutboxEvent): Promise<void> {
    const message = {
      id: event.id,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.payload,
    };
    const accepted = this.channel.publish(
      this.exchange,
      event.eventType,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
      },
    );

    if (!accepted) {
      await new Promise<void>((resolve, reject) => {
        const onDrain = (): void => {
          this.channel.removeListener("error", onError);
          resolve();
        };
        const onError = (error: Error): void => {
          this.channel.removeListener("drain", onDrain);
          reject(error);
        };
        this.channel.once("drain", onDrain);
        this.channel.once("error", onError);
      });
    }
  }
}
