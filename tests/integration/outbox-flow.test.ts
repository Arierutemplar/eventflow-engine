import amqp, {
  type Channel,
  type ChannelModel,
} from "amqplib";
import { randomUUID } from "node:crypto";
import { database } from "@eventflow/database";
import {
  DEAD_LETTER_QUEUE,
  RETRY_QUEUES,
  RabbitMQClient,
} from "@eventflow/event-bus";
import { executeIdempotently } from "../../apps/event-workers/src/services/idempotency.service.js";
import { createEventWithOutbox } from "../../apps/event-workers/src/services/event-creation.service.js";

const requireEnvironmentVariable = (name: "DATABASE_URL" | "RABBITMQ_URL"): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} debe estar configurada para ejecutar las pruebas de integración`);
  }
  return value;
};

describe("Outbox flow E2E", () => {
  let rabbit: RabbitMQClient;
  let connection: ChannelModel;
  let channel: Channel;
  const temporaryQueues = new Set<string>();

  beforeAll(async () => {
    requireEnvironmentVariable("DATABASE_URL");
    const rabbitUrl = requireEnvironmentVariable("RABBITMQ_URL");

    await database.$connect();
    rabbit = new RabbitMQClient(rabbitUrl);
    await rabbit.connect();
    connection = await amqp.connect(rabbitUrl);
    channel = await connection.createChannel();
  });

  beforeEach(async () => {
    await database.$transaction([
      database.outboxMessage.deleteMany(),
      database.idempotencyRecord.deleteMany(),
      database.event.deleteMany(),
    ]);
  });

  afterAll(async () => {
    for (const queue of temporaryQueues) {
      await channel?.deleteQueue(queue);
    }
    await channel?.close();
    await connection?.close();
    await rabbit?.close();
    await database.$disconnect();
  });

  it("persists a business event and a pending Outbox message atomically", async () => {
    const eventType = "PRODUCT_CREATED";
    const payload = {
      productId: randomUUID(),
      name: "Integration Product",
      price: 2500,
    };

    const event = await createEventWithOutbox({ type: eventType, payload });
    const [persistedEvent, outboxMessage] = await Promise.all([
      database.event.findUnique({ where: { id: event.id } }),
      database.outboxMessage.findFirst({
        where: { eventType },
      }),
    ]);

    expect(persistedEvent).toMatchObject({
      id: event.id,
      type: eventType,
      payload,
      status: "PENDING",
    });
    expect(outboxMessage).toMatchObject({
      eventType,
      payload,
      published: false,
      sentAt: null,
    });
  });

  it("ACKs a duplicate event without executing its side effects", async () => {
    const eventId = randomUUID();
    const queueName = `integration-idempotency-${eventId}`;
    temporaryQueues.add(queueName);
    let sideEffects = 0;

    await database.idempotencyRecord.create({
      data: {
        key: eventId,
        status: "COMPLETED",
        response: { accepted: true },
      },
    });
    await rabbit.publishToQueue(queueName, {
      eventId,
      eventType: "PRODUCT_CREATED",
    });

    const message = await channel.get(queueName, { noAck: false });
    expect(message).not.toBe(false);
    if (message === false) {
      throw new Error(`No se recibió el evento duplicado en ${queueName}`);
    }

    const received = JSON.parse(message.content.toString()) as { eventId: string };
    const result = await executeIdempotently(received.eventId, async () => {
      sideEffects += 1;
      return { accepted: true };
    });
    channel.ack(message);

    expect(result).toMatchObject({
      replayed: true,
      result: { accepted: true },
    });
    expect(sideEffects).toBe(0);
    await expect(channel.get(queueName, { noAck: true })).resolves.toBe(false);
  });

  it("routes transient failures through retry queues and finally to the DLQ", async () => {
    const eventId = randomUUID();
    const routingKey = `integration.failure.${eventId}`;
    const event = {
      eventId,
      eventType: "PRODUCT_CREATED",
      payload: { productId: randomUUID() },
    };
    const headers: Record<string, unknown> = {
      "x-test-event-id": eventId,
    };

    for (let attempt = 1; attempt <= RETRY_QUEUES.length; attempt += 1) {
      const retryQueue = RETRY_QUEUES[attempt - 1];
      if (!retryQueue) {
        throw new Error(`No existe la cola de reintento para el intento ${attempt}`);
      }

      await expect(
        rabbit.publishToRetry(attempt, routingKey, event, headers),
      ).resolves.toBe(retryQueue.name);

      const retryMessage = await channel.get(retryQueue.name, { noAck: false });
      expect(retryMessage).not.toBe(false);
      if (retryMessage === false) {
        throw new Error(`No se recibió el mensaje en ${retryQueue.name}`);
      }

      expect(JSON.parse(retryMessage.content.toString())).toMatchObject(event);
      expect(retryQueue.ttlMs).toBeGreaterThan(0);
      channel.ack(retryMessage);
    }

    await rabbit.publishToDeadLetter(routingKey, event, headers);
    const deadLetterMessage = await channel.get(DEAD_LETTER_QUEUE, {
      noAck: false,
    });
    expect(deadLetterMessage).not.toBe(false);
    if (deadLetterMessage === false) {
      throw new Error(`No se recibió el mensaje en ${DEAD_LETTER_QUEUE}`);
    }

    expect(JSON.parse(deadLetterMessage.content.toString())).toMatchObject(event);
    channel.ack(deadLetterMessage);
  });
});
