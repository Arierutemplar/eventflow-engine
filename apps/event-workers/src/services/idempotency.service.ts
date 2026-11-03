import { database } from "@eventflow/database";
import { Prisma, PrismaClient } from "@prisma/client";

export class IdempotencyService {
  constructor(private readonly prisma: PrismaClient) {}

  async acquireLock(eventId: string): Promise<boolean> {
    const normalizedEventId = eventId.trim();
    if (!normalizedEventId) {
      throw new Error("eventId no puede estar vacío");
    }

    try {
      await this.prisma.idempotencyRecord.create({
        data: {
          eventId: normalizedEventId,
          status: IDEMPOTENCY_STATUS.PROCESSING,
        },
      });
      return true;
    } catch (error: unknown) {
      if (isUniqueViolation(error)) return false;
      throw error;
    }
  }

  async markAsCompleted(
    eventId: string,
    result?: Prisma.InputJsonValue,
  ): Promise<void> {
    await this.prisma.idempotencyRecord.update({
      where: { eventId },
      data: {
        status: IDEMPOTENCY_STATUS.COMPLETED,
        result: result ?? Prisma.JsonNull,
      },
    });
  }

  async markAsFailed(eventId: string, errorDetails: string): Promise<void> {
    await this.prisma.idempotencyRecord.update({
      where: { eventId },
      data: {
        status: IDEMPOTENCY_STATUS.FAILED,
        result: { error: errorDetails },
      },
    });
  }
}

export const IDEMPOTENCY_STATUS = {
  PROCESSING: "PROCESSING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
} as const;

export class IdempotencyConflictError extends Error {
  constructor(key: string) {
    super(`La clave de idempotencia "${key}" ya está siendo procesada`);
    this.name = "IdempotencyConflictError";
  }
}

export type IdempotencyResult<T> = {
  result: T;
  replayed: boolean;
};

export async function executeIdempotently<T>(
  idempotencyKey: string,
  operation: () => Promise<T>,
): Promise<IdempotencyResult<T>> {
  const key = idempotencyKey.trim();
  if (!key) throw new Error("idempotencyKey no puede estar vacío");

  try {
    await database.$transaction(async (tx) => {
      await tx.idempotencyRecord.create({
        data: { key, status: IDEMPOTENCY_STATUS.PROCESSING },
      });
    });
  } catch (error) {
    if (!isUniqueViolation(error)) throw error;

    const existing = await database.idempotencyRecord.findUnique({ where: { key } });
    if (!existing) {
      throw new Error(`No se pudo resolver la reserva de idempotencia "${key}"`);
    }

    if (existing.status === IDEMPOTENCY_STATUS.COMPLETED) {
      return {
        result: existing.response as T,
        replayed: true,
      };
    }

    if (existing.status === IDEMPOTENCY_STATUS.FAILED) {
      const claimed = await database.idempotencyRecord.updateMany({
        where: { key, status: IDEMPOTENCY_STATUS.FAILED },
        data: {
          status: IDEMPOTENCY_STATUS.PROCESSING,
          response: Prisma.JsonNull,
        },
      });
      if (claimed.count === 1) {
        return executeOperation(key, operation);
      }
    }

    throw new IdempotencyConflictError(key);
  }

  return executeOperation(key, operation);
}

async function executeOperation<T>(
  key: string,
  operation: () => Promise<T>,
): Promise<IdempotencyResult<T>> {
  try {
    const result = await operation();
    await database.idempotencyRecord.update({
      where: { key },
      data: {
        status: IDEMPOTENCY_STATUS.COMPLETED,
        response: result as Prisma.InputJsonValue,
      },
    });
    return { result, replayed: false };
  } catch (error) {
    await database.idempotencyRecord.update({
      where: { key },
      data: { status: IDEMPOTENCY_STATUS.FAILED },
    });
    throw error;
  }
}

function isUniqueViolation(error: unknown): boolean {
  return error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002";
}
