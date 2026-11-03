import type { PrismaClient, Product } from "@prisma/client";
import type { ProductRepository } from "./product.repository.js";
import type { CreateProductInput } from "./product.types.js";
import { CircuitBreaker } from "@eventflow/common";

export class ProductService {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly products: ProductRepository,
  ) {}

  createProduct(input: CreateProductInput): Promise<Product> {
    const normalizedInput: CreateProductInput = {
      name: input.name.trim(),
      price: input.price,
    };

    if (input.status !== undefined) {
      normalizedInput.status = input.status.trim();
    }

    return this.databaseBreaker.execute(() => this.prisma.$transaction(async (tx) => {
      const product = await this.products.create(normalizedInput, tx);
      await tx.outboxMessage.create({
        data: {
          eventType: "product.created",
          payload: {
            productId: product.id,
            name: product.name,
            price: product.price,
            status: product.status,
          },
          published: false,
          createdAt: new Date(),
        },
      });

      return product;
    }), async () => {
      throw new Error("La base de datos no está disponible temporalmente");
    });
  }

  listProducts(): Promise<Product[]> {
    return this.databaseBreaker.execute(
      () => this.products.list(),
      async () => [],
    );
  }

  readonly databaseBreaker = new CircuitBreaker({
    name: "api-gateway-database",
    volumeThreshold: 5,
    errorThresholdPercentage: 50,
    timeoutMs: 5_000,
    resetTimeoutMs: 10_000,
  });
}
