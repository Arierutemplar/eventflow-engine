import type { Product } from "@prisma/client";

export type CreateProductInput = {
  name: string;
  price: number;
  status?: string;
};

export type ProductResponse = {
  success: true;
  data: Product;
};

export type ProductListResponse = {
  success: true;
  data: Product[];
};
