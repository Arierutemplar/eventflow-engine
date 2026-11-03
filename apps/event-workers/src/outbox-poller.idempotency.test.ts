import { prisma } from "@eventflow/database";
import { Prisma } from "@prisma/client";
import { OutboxProcessor } from "./outbox-poller.js";

describe("OutboxProcessor idempotency", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("publishes an event once and skips the duplicate attempt", async () => {
    const message = {
      id: "outbox-1",
      eventType: "order.created",
      payload: { orderId: "order-1" },
      published: false,
      createdAt: new Date("2026-11-01T22:00:00.000Z"),
      sentAt: null,
    };
    const publishEvent = jest.fn().mockResolvedValue(undefined);
    const idempotencyCreate = jest
      .spyOn(prisma.idempotencyRecord, "create")
      .mockResolvedValueOnce({
        id: "idempotency-1",
        eventId: null,
        key: "outbox:outbox-1",
        status: "PROCESSING",
        result: null,
        response: null,
        createdAt: message.createdAt,
        updatedAt: message.createdAt,
      })
      .mockRejectedValueOnce(
        new Prisma.PrismaClientKnownRequestError("duplicate key", {
          code: "P2002",
          clientVersion: "test",
        }),
      );
    const idempotencyUpdate = jest
      .spyOn(prisma.idempotencyRecord, "update")
      .mockResolvedValue({
        id: "idempotency-1",
        eventId: null,
        key: "outbox:outbox-1",
        status: "COMPLETED",
        result: null,
        response: { published: true },
        createdAt: message.createdAt,
        updatedAt: message.createdAt,
      });
    const idempotencyFind = jest
      .spyOn(prisma.idempotencyRecord, "findUnique")
      .mockResolvedValue({
        id: "idempotency-1",
        eventId: null,
        key: "outbox:outbox-1",
        status: "COMPLETED",
        result: null,
        response: { published: true },
        createdAt: message.createdAt,
        updatedAt: message.createdAt,
      });
    const outboxFind = jest
      .spyOn(prisma.outboxMessage, "findMany")
      .mockResolvedValue([message]);
    const outboxUpdate = jest
      .spyOn(prisma.outboxMessage, "updateMany")
      .mockResolvedValue({ count: 1 });
    const transaction = jest
      .spyOn(prisma, "$transaction")
      .mockImplementation(
        (async (callback: (tx: Prisma.TransactionClient) => Promise<unknown>) =>
          callback(prisma as never)) as never,
      );

    const processor = new OutboxProcessor(
      { publishEvent } as never,
      { batchSize: 10, intervalMs: 5_000 },
    );

    await processor.processPendingMessages();
    await processor.processPendingMessages();

    expect(transaction).toHaveBeenCalledTimes(2);
    expect(idempotencyCreate).toHaveBeenCalledTimes(2);
    expect(idempotencyFind).toHaveBeenCalledWith({
      where: { key: "outbox:outbox-1" },
    });
    expect(idempotencyUpdate).toHaveBeenCalledTimes(1);
    expect(publishEvent).toHaveBeenCalledTimes(1);
    expect(outboxFind).toHaveBeenCalledTimes(2);
    expect(outboxUpdate).toHaveBeenCalledTimes(2);
  });
});
