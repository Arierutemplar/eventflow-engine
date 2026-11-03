import amqp, {
  type Channel,
  type ChannelModel,
  type ConsumeMessage,
} from "amqplib";
import { randomUUID } from "node:crypto";
import { database } from "@eventflow/database";
import {
  DEAD_LETTER_QUEUE,
  RETRY_QUEUES,
  RabbitMQClient,
} from "@eventflow/event-bus";
import { executeIdempotently } from "../../apps/event-workers/src/services/idempotency.service.js";
import { OutboxPoller } from "../../apps/event-workers/src/outbox-poller.js";

const rabbitUrl =
  process.env.RABBITMQ_URL ?? "******localhost:5672";

let rabbit: RabbitMQClient;
let connection: ChannelModel;
let channel: Channel;
const createdProductIds: string[] = [];

function waitForMessage(
  queueName: string,
  predicate: (message: ConsumeMessage) => boolean,
  timeoutMs = 10_000,
): Promise<ConsumeMessage> {
  return new Promise((resolve, reject) => {
    let consumerTag: string | undefined;
    const timer = setTimeout(() => {
      if (consumerTag) void channel.cancel(consumerTag);
      reject(new Error(`Timeout esperando mensajes en ${queueName}`));
    }, timeoutMs);

    void channel.consume(queueName, (message) => {
      if (!message) return;
      if (!predicate(message)) {
        channel.ack(message);
        return;
      }
      clearTimeout(timer);
      consumerTag = message.fields.consumerTag;
      channel.ack(message);
      resolve(message);
    }).then(({ consumerTag: tag }) => {
      consumerTag = tag;
    }).catch(reject);
  });
}

describe("Eventflow event flow E2E", () => {
  beforeAll(async () => {
    await database.$connect();
    rabbit = RabbitMQClient.getInstance(rabbitUrl);
    await rabbit.connect();
    connection = await amqp.connect(rabbitUrl);
    channel = await connection.createChannel();
    await channel.assertQueue("events_queue", {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "eventflow.events.dlx",
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
      },
    });
    await channel.bindQueue("events_queue", "eventflow.events", "#");
  });

  afterAll(async () => {
    if (createdProductIds.length > 0) {
      await database.product.deleteMany({
        where: { id: { in: createdProductIds } },
      });
    }
    await database.outboxMessage.deleteMany();
    await database.idempotencyRecord.deleteMany();
    await channel?.close();
    await connection?.close();
    await rabbit?.close();
    await database.$disconnect();
  });

  it("persists Product and Outbox, then delivers the event through RabbitMQ", async () => {
    const productId = randomUUID();
    const eventId = randomUUID();
    createdProductIds.push(productId);

    await database.$transaction(async (tx) => {
      await tx.product.create({
        data: { id: productId, name: "E2E Product", price: 2500 },
      });
      await tx.outboxMessage.create({
        data: {
          id: eventId,
          eventType: "product.created",
          payload: { productId, name: "E2E Product", price: 2500 },
        },
      });
    });

    const pending = await database.outboxMessage.findUnique({
      where: { id: eventId },
    });
    expect(pending?.published).toBe(false);

    const poller = new OutboxPoller(rabbit, { intervalMs: 60_000, batchSize: 10 });
    await poller.processPendingMessages();

    const message = await waitForMessage(
      "events_queue",
      (candidate) => {
        const event = JSON.parse(candidate.content.toString()) as {
          id?: string;
          eventId?: string;
        };
        return event.id === eventId || event.eventId === eventId;
      },
    );
    expect(JSON.parse(message.content.toString()).id).toBe(eventId);
    await expect(
      database.outboxMessage.findUnique({ where: { id: eventId } }),
    ).resolves.toMatchObject({ published: true });
  });

  it("records idempotency and ACKs a duplicate event without processing twice", async () => {
    const eventId = randomUUID();
    let executions = 0;
    const operation = async () => {
      executions += 1;
      return { eventId };
    };

    await executeIdempotently(eventId, operation);
    const replay = await executeIdempotently(eventId, operation);

    expect(executions).toBe(1);
    expect(replay.replayed).toBe(true);
    await expect(
      database.idempotencyRecord.findUnique({ where: { key: eventId } }),
    ).resolves.toMatchObject({ key: eventId, status: "COMPLETED" });
  });

  it("routes a transient failure through retries and then to the DLQ", async () => {
      const eventId = randomUUID();
      const routingKey = "product.created";
      let headers: Record<string, unknown> = {};
      const event = { eventId, eventType: routingKey, payload: { name: "FAIL_TEST" } };

      for (let attempt = 1; attempt <= RETRY_QUEUES.length; attempt += 1) {
        const retryQueue = RETRY_QUEUES[attempt - 1];
        if (!retryQueue) throw new Error(`Retry queue ${attempt} no configurada`);
        await rabbit.publishToRetry(attempt, routingKey, event, headers);
        const returned = await channel.get(retryQueue.name, { noAck: false });
        expect(returned).not.toBe(false);
        if (returned === false) throw new Error(`Mensaje no enrutado a ${retryQueue.name}`);
        channel.ack(returned);
        headers =
          typeof returned.properties.headers === "object" &&
          returned.properties.headers !== null
            ? { ...(returned.properties.headers as Record<string, unknown>) }
            : {};
        expect(retryQueue.ttlMs).toBeGreaterThan(0);
      }

      await rabbit.publishToDeadLetter(routingKey, event, headers);
      await channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
      const deadLetter = await channel.get(DEAD_LETTER_QUEUE, { noAck: false });
      expect(deadLetter).not.toBe(false);
      if (deadLetter === false) throw new Error("Mensaje no enrutado a la DLQ");
      channel.ack(deadLetter);
      expect(JSON.parse(deadLetter.content.toString()).eventId).toBe(eventId);
    });
});
