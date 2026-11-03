import { z } from "zod";

export const eventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  occurredAt: z.string().datetime(),
  version: z.number().int().positive(),
  payload: z.unknown(),
  correlationId: z.string().min(1).optional(),
});