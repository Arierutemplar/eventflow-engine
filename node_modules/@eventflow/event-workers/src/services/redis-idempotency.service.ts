import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";

const RELEASE_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  end
  return 0
`;

const COMPLETE_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("SET", KEYS[1], "processed", "EX", ARGV[2])
  end
  return nil
`;

export class RedisIdempotencyService {
  private readonly redis: Redis;
  private readonly keyPrefix: string;
  private readonly ttlSeconds: number;
  private readonly tokens = new Map<string, string>();

  constructor(
    redisUrl = process.env.REDIS_URL,
    ttlSeconds = Number(process.env.REDIS_IDEMPOTENCY_TTL_SECONDS ?? 86_400),
  ) {
    if (!redisUrl) {
      throw new Error("REDIS_URL es obligatoria para la idempotencia Redis");
    }
    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
      throw new Error("REDIS_IDEMPOTENCY_TTL_SECONDS debe ser positivo");
    }

    this.redis = new Redis(redisUrl, {
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1,
      lazyConnect: true,
    });
    this.keyPrefix = process.env.REDIS_IDEMPOTENCY_PREFIX ?? "eventflow:idempotency";
    this.ttlSeconds = ttlSeconds;
  }

  async acquire(eventId: string): Promise<boolean> {
    const normalizedEventId = eventId.trim();
    if (!normalizedEventId) {
      throw new Error("eventId no puede estar vacío");
    }

    const token = randomUUID();
    const result = await this.redis.set(
      this.getKey(normalizedEventId),
      token,
      "EX",
      this.ttlSeconds,
      "NX",
    );
    if (result !== "OK") return false;

    this.tokens.set(normalizedEventId, token);
    return true;
  }

  async tryAcquire(eventId: string): Promise<boolean | null> {
    try {
      return await this.acquire(eventId);
    } catch {
      return null;
    }
  }

  async markProcessed(eventId: string): Promise<void> {
    const normalizedEventId = eventId.trim();
    const token = this.tokens.get(normalizedEventId);
    if (!token) return;

    await this.redis.eval(
      COMPLETE_LOCK_SCRIPT,
      1,
      this.getKey(normalizedEventId),
      token,
      String(this.ttlSeconds),
    );
    this.tokens.delete(normalizedEventId);
  }

  async release(eventId: string): Promise<void> {
    const normalizedEventId = eventId.trim();
    const token = this.tokens.get(normalizedEventId);
    if (!token) return;

    try {
      await this.redis.eval(
        RELEASE_LOCK_SCRIPT,
        1,
        this.getKey(normalizedEventId),
        token,
      );
    } finally {
      this.tokens.delete(normalizedEventId);
    }
  }

  async close(): Promise<void> {
    this.tokens.clear();
    await this.redis.quit();
  }

  private getKey(eventId: string): string {
    return `${this.keyPrefix}:${eventId}`;
  }
}
