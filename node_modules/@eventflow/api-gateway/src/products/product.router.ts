import type { FastifyInstance } from "fastify";
import type { PrismaClient } from "@prisma/client";
import { ProductController } from "./product.controller.js";
import { ProductRepository } from "./product.repository.js";
import { ProductService } from "./product.service.js";
import { registerCircuitBreakerMetrics, startMetricsServer } from "../metrics.js";

type CreateProductBody = {
  name: string;
  price: number;
  status?: string;
};

export async function registerProductRoutes(
  app: FastifyInstance,
  prisma: PrismaClient,
): Promise<void> {
  const repository = new ProductRepository(prisma);
  const service = new ProductService(prisma, repository);
  const controller = new ProductController(service);
  const collectMetrics = registerCircuitBreakerMetrics(service.databaseBreaker);
  startMetricsServer(collectMetrics);

  app.post<{ Body: CreateProductBody }>(
    "/products",
    {
      schema: {
        body: {
          type: "object",
          required: ["name", "price"],
          additionalProperties: false,
          properties: {
            name: { type: "string", minLength: 1 },
            price: { type: "integer", minimum: 0 },
            status: { type: "string", minLength: 1 },
          },
        },
      },
    },
    (request, reply) => controller.create(request, reply),
  );

  app.get("/products", (request, reply) => controller.list(request, reply));
}
