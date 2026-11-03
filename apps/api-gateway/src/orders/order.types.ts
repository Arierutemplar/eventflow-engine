export type CreateOrderInput = {
  customerId: string;
  total: number;
};

export type OrderCreatedEvent = {
  orderId: string;
  customerId: string;
  total: number;
};
