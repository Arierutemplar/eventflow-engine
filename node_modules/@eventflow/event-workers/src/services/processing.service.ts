import { logger } from "@eventflow/common";

export async function processEventPayload(payload: unknown): Promise<void> {
  logger.debug({ payload }, "Procesando evento");
}