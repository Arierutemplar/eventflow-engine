import type { Prisma, PrismaClient } from "@prisma/client";
import { OrderService } from "./order.service.js";
import type { OrderRepository } from "./order.repository.js";

describe("OrderService", () => {
  const order = {
    id: "order-1",
    customerId: "customer-1",
    total: 2500,
    status: "PENDING",
    createdAt: new Date("2026-11-01T22:00:00.000Z"),
    updatedAt: new Date("2026-11-01T22:00:00.000Z"),
  };
  const input = {
    customerId: "customer-1",
    total: 2500,
  };

  function createTestContext() {
    const repository = {
      create: jest.fn().mockResolvedValue(order),
    } as unknown as jest.Mocked<OrderRepository>;
    const tx = {
      outboxMessage: {
        create: jest.fn().mockResolvedValue({
          id: "outbox-1",
          eventType: "order.created",
          payload: {
            orderId: order.id,
            customerId: order.customerId,
            total: order.total,
          },
          published: false,
          createdAt: order.createdAt,
          sentAt: null,
        }),
      },
    } as unknown as Prisma.TransactionClient;
    const prisma = {
      $transaction: jest.fn(
        async (
          callback: (transaction: Prisma.TransactionClient) => Promise<unknown>,
        ) => callback(tx),
      ),
    } as unknown as PrismaClient;

    return { repository, tx, prisma };
  }

  it("creates the order and its outbox message in one transaction", async () => {
    const { repository, tx, prisma } = createTestContext();
    const service = new OrderService(prisma, repository);

    const result = await service.createOrder(input);

    expect(result).toEqual(order);
    expect(prisma.$transaction).toHaveBeenCalledTimes(1);
    expect(repository.create).toHaveBeenCalledWith(input, tx);
    expect(tx.outboxMessage.create).toHaveBeenCalledWith({
      data: {
        eventType: "order.created",
        payload: {
          orderId: "order-1",
          customerId: "customer-1",
          total: 2500,
        },
        published: false,
        createdAt: expect.any(Date),
      },
    });
  });

  it("propagates repository failures and does not create the outbox message", async () => {
    const { repository, tx, prisma } = createTestContext();
    const failure = new Error("order insert failed");
    repository.create.mockRejectedValueOnce(failure);
    const service = new OrderService(prisma, repository);

    await expect(service.createOrder(input)).rejects.toThrow("order insert failed");
    expect(tx.outboxMessage.create).not.toHaveBeenCalled();
  });
});
