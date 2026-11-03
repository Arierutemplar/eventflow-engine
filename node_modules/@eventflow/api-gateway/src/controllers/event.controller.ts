import { eventSchema } from "@eventflow/common";
import { z } from "zod";

type ValidatedEvent = z.infer<typeof eventSchema>;

export function validateEvent(input: unknown): ValidatedEvent {
  return eventSchema.parse(input);
}