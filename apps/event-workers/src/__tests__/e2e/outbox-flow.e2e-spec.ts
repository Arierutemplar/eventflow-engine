import amqp, {
  type Channel,
  type ChannelModel,
} from "amqplib";
import { randomUUID } from "node:crypto";
import { PrismaClient } from "@prisma/client";
import { EVENT_EXCHANGE } from "@eventflow/event-bus";
import { IdempotencyService } from "../../services/idempotency.service.js";
import { OutboxPollerWorker } from "../../workers/outbox-poller.worker.js";

const requireEnvironmentVariable = (name: "DATABASE_URL" | "RABBITMQ_URL"): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} debe estar configurada para ejecutar la prueba E2E`);
  }
  return value;
};

async function waitForPublished(
  prisma: PrismaClient,
  eventId: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const event = await prisma.outboxEvent.findUnique({
      where: { id: eventId },
      select: { status: true },
    });
    if (event?.status === "PUBLISHED") return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timeout esperando la publicación del evento ${eventId}`);
}

describe("Transactional Outbox e Idempotencia E2E", () => {
  let prisma: PrismaClient;
  let idempotencyService: IdempotencyService;
  let connection: ChannelModel;
  let channel: Channel;
  let worker: OutboxPollerWorker;
  let testQueue: string;

  beforeAll(async () => {
    const databaseUrl = requireEnvironmentVariable("DATABASE_URL");
    const rabbitUrl = requireEnvironmentVariable("RABBITMQ_URL");

    prisma = new PrismaClient({ datasources: { db: { url: databaseUrl } } });
    idempotencyService = new IdempotencyService(prisma);
    await prisma.$connect();

    connection = await amqp.connect(rabbitUrl);
    channel = await connection.createChannel();
    await channel.assertExchange(EVENT_EXCHANGE, "topic", { durable: true });

    testQueue = `e2e-outbox-${randomUUID()}`;
    await channel.assertQueue(testQueue, { durable: false, autoDelete: true });
    await channel.bindQueue(testQueue, EVENT_EXCHANGE, "#");
  });

  beforeEach(async () => {
    await prisma.$transaction([
      prisma.idempotencyRecord.deleteMany(),
      prisma.outboxEvent.deleteMany(),
    ]);
  });

  afterAll(async () => {
    await worker?.stop();
    if (channel && testQueue) {
      await channel.deleteQueue(testQueue);
      await channel.close();
    }
    await connection?.close();
    await prisma?.$disconnect();
  });

  it("publica el Outbox y procesa el evento una sola vez", async () => {
    const eventId = randomUUID();
    const aggregateId = randomUUID();
    const eventType = "product.created";
    const payload = { productId: aggregateId, name: "E2E Product" };

    await prisma.outboxEvent.create({
      data: {
        id: eventId,
        aggregateId,
        eventType,
        payload,
        status: "PENDING",
      },
    });

    worker = new OutboxPollerWorker(prisma, channel, {
      intervalMs: 25,
      batchSize: 10,
    });
    worker.start();
    await waitForPublished(prisma, eventId);
    await worker.stop();

    await expect(
      prisma.outboxEvent.findUnique({
        where: { id: eventId },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: "PUBLISHED" });

    const message = await channel.get(testQueue, { noAck: false });
    expect(message).not.toBe(false);
    if (message === false) {
      throw new Error("No se recibió el evento publicado en RabbitMQ");
    }

    const published = JSON.parse(message.content.toString()) as {
      id: string;
      aggregateId: string;
      eventType: string;
      payload: typeof payload;
    };
    expect(published).toEqual({ id: eventId, aggregateId, eventType, payload });
    channel.ack(message);

    await expect(idempotencyService.acquireLock(eventId)).resolves.toBe(true);
    await idempotencyService.markAsCompleted(eventId, {
      processed: true,
    });

    await expect(idempotencyService.acquireLock(eventId)).resolves.toBe(false);
    await expect(
      prisma.idempotencyRecord.findUnique({
        where: { eventId },
        select: { status: true, result: true },
      }),
    ).resolves.toMatchObject({
      status: "COMPLETED",
      result: { processed: true },
    });
  });
});
