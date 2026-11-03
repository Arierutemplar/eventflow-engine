import { database } from "@eventflow/database";
import { Prisma } from "@prisma/client";

export async function executeNotificationOnce<T>(
  eventId: string,
  operation: () => Promise<T>,
): Promise<{ result: T; duplicate: boolean }> {
  try {
    await database.idempotencyRecord.create({
      data: { key: `notification:${eventId}`, status: "PROCESSING" },
    });
  } catch (error) {
    if (!isUniqueViolation(error)) throw error;

    const record = await database.idempotencyRecord.findUnique({
      where: { key: `notification:${eventId}` },
    });
    if (record?.status === "COMPLETED") {
      return { result: record.response as T, duplicate: true };
    }
    throw new Error(`El evento ${eventId} está siendo procesado`);
  }

  try {
    const result = await operation();
    await database.idempotencyRecord.update({
      where: { key: `notification:${eventId}` },
      data: {
        status: "COMPLETED",
        response: result as Prisma.InputJsonValue,
      },
    });
    return { result, duplicate: false };
  } catch (error) {
    await database.idempotencyRecord.update({
      where: { key: `notification:${eventId}` },
      data: { status: "FAILED" },
    });
    throw error;
  }
}

function isUniqueViolation(error: unknown): boolean {
  return (
    error instanceof Prisma.PrismaClientKnownRequestError &&
    error.code === "P2002"
  );
}
