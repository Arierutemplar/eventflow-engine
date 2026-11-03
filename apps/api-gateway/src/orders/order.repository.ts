import type { Order, Prisma, PrismaClient } from "@prisma/client";
import type { CreateOrderInput } from "./order.types.js";

export type PrismaTransactionClient = Prisma.TransactionClient;
type PrismaExecutor = PrismaClient | PrismaTransactionClient;

export class OrderRepository {
  constructor(private readonly prisma: PrismaClient) {}

  create(
    input: CreateOrderInput,
    tx?: PrismaTransactionClient,
  ): Promise<Order> {
    const client: PrismaExecutor = tx ?? this.prisma;
    return client.order.create({
      data: {
        customerId: input.customerId,
        total: input.total,
      },
    });
  }
}
