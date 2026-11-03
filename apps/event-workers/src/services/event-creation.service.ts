import { database } from "@eventflow/database";
import type { Prisma } from "@prisma/client";

export type CreateEventInput = {
  type: string;
  payload: Prisma.InputJsonValue;
};

export async function createEventWithOutbox(
  input: CreateEventInput,
): Promise<Prisma.EventGetPayload<object>> {
  return database.$transaction(async (tx) => {
    const event = await tx.event.create({
      data: {
        type: input.type,
        payload: input.payload,
      },
    });

    await tx.outboxMessage.create({
      data: {
        eventType: input.type,
        payload: input.payload,
        published: false,
        createdAt: new Date(),
      },
    });

    return event;
  });
}
