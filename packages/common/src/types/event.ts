export type EventEnvelope<T = unknown> = {
  id: string;
  type: string;
  occurredAt: string;
  version: number;
  payload: T;
  correlationId?: string;
};