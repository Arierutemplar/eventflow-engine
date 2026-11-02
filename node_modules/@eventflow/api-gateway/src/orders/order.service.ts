import type { PrismaClient } from "@prisma/client";
import type { OrderRepository } from "./order.repository.js";
import type { CreateOrderInput, OrderCreatedEvent } from "./order.types.js";

export class OrderService {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly orders: OrderRepository,
  ) {}

  async createOrder(input: CreateOrderInput) {
    return this.prisma.$transaction(async (tx) => {
      const order = await this.orders.create(input, tx);
      const event: OrderCreatedEvent = {
        orderId: order.id,
        customerId: order.customerId,
        total: order.total,
      };

      await tx.outboxMessage.create({
        data: {
          eventType: "order.created",
          payload: event,
          published: false,
          createdAt: new Date(),
        },
      });

      return order;
    });
  }
}
