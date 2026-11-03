import type { Prisma, PrismaClient, Product } from "@prisma/client";
import type { CreateProductInput } from "./product.types.js";

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

export class ProductRepository {
  constructor(private readonly prisma: PrismaClient) {}

  create(
    input: CreateProductInput,
    tx?: Prisma.TransactionClient,
  ): Promise<Product> {
    const client: PrismaExecutor = tx ?? this.prisma;
    return client.product.create({
      data: {
        name: input.name,
        price: input.price,
        ...(input.status === undefined ? {} : { status: input.status }),
      },
    });
  }

  list(): Promise<Product[]> {
    return this.prisma.product.findMany({
      orderBy: { createdAt: "desc" },
    });
  }
}
