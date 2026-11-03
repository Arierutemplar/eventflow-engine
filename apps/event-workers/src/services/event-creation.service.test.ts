import { database } from "@eventflow/database";
import type { Prisma } from "@prisma/client";
import { createEventWithOutbox } from "./event-creation.service.js";

describe("createEventWithOutbox", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("rolls back the business entity when Outbox insertion fails", async () => {
    const transaction = jest.spyOn(database, "$transaction");
    const eventCreate = jest.spyOn(database.event, "create").mockResolvedValue({
      id: "event-test",
      type: "order.created",
      payload: { orderId: "order-test" },
      status: "PENDING",
      attempts: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const outboxCreate = jest
      .spyOn(database.outboxMessage, "create")
      .mockRejectedValueOnce(new Error("outbox unavailable"));
    const eventCount = jest.spyOn(database.event, "count").mockResolvedValue(0);

    transaction.mockImplementation(
      (async (callback: (tx: Prisma.TransactionClient) => Promise<unknown>) =>
        callback(database as never)) as never,
    );

    await expect(
      createEventWithOutbox({
        type: "order.created",
        payload: { orderId: "order-test" },
      }),
    ).rejects.toThrow("outbox unavailable");

    expect(eventCreate).toHaveBeenCalledTimes(1);
    expect(outboxCreate).toHaveBeenCalledTimes(1);
    await expect(database.event.count()).resolves.toBe(0);
    expect(eventCount).toHaveBeenCalledTimes(1);
  });
});
