import type { FastifyReply, FastifyRequest } from "fastify";
import type { ProductService } from "./product.service.js";
import type {
  CreateProductInput,
  ProductListResponse,
  ProductResponse,
} from "./product.types.js";

export class ProductController {
  constructor(private readonly products: ProductService) {}

  async create(
    request: FastifyRequest<{ Body: CreateProductInput }>,
    reply: FastifyReply,
  ): Promise<ProductResponse> {
    try {
      const product = await this.products.createProduct(request.body);
      return reply.code(201).send({ success: true, data: product });
    } catch (error) {
      request.log.error(error, "Product creation failed");
      throw error;
    }
  }

  async list(
    request: FastifyRequest,
    reply: FastifyReply,
  ): Promise<ProductListResponse> {
    try {
      const products = await this.products.listProducts();
      return reply.send({ success: true, data: products });
    } catch (error) {
      request.log.error(error, "Product listing failed");
      throw error;
    }
  }
}
