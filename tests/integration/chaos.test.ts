import type { Channel } from "amqplib";
import type { NextFunction, Request, Response } from "express";
import { OutboxPollerWorker } from "../../apps/event-workers/src/workers/outbox-poller.worker.js";
import { RedisIdempotencyService } from "../../apps/event-workers/src/services/redis-idempotency.service.js";

type RedisMock = {
  set: jest.Mock;
  eval: jest.Mock;
  quit: jest.Mock;
};

const mockRedis: RedisMock = {
  set: jest.fn(),
  eval: jest.fn(),
  quit: jest.fn().mockResolvedValue("OK"),
};

jest.mock("ioredis", () => ({
  Redis: jest.fn(() => mockRedis),
}));

describe("Chaos resilience", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("Redis idempotency failure", () => {
    it("degrades fail-open when Redis is unavailable", async () => {
      mockRedis.set.mockRejectedValueOnce(new Error("Redis connection refused"));
      const service = new RedisIdempotencyService("redis://unavailable", 60);

      await expect(service.tryAcquire("event-redis-down")).resolves.toBeNull();
      expect(mockRedis.set).toHaveBeenCalledWith(
        "eventflow:idempotency:event-redis-down",
        expect.any(String),
        "EX",
        60,
        "NX",
      );

      await service.close();
      expect(mockRedis.quit).toHaveBeenCalledTimes(1);
    });

    it("allows requests when Redis fails inside the rate limiter", async () => {
      const originalRedisUrl = process.env.REDIS_URL;
      process.env.REDIS_URL = "redis://unavailable";
      jest.doMock("rate-limiter-flexible", () => ({
        RateLimiterRedis: jest.fn(() => ({
          consume: jest.fn().mockRejectedValue(new Error("Redis unavailable")),
        })),
      }));

      try {
        let middleware:
          | ((request: Request, response: Response, next: NextFunction) => Promise<void>)
          | undefined;
        jest.isolateModules(() => {
          middleware = require("../../apps/api-gateway/src/middlewares/rate-limiter.middleware.js")
            .rateLimiterMiddleware as typeof middleware;
        });

        const next = jest.fn();
        const response = {
          setHeader: jest.fn(),
          status: jest.fn(),
          json: jest.fn(),
        } as unknown as Response;
        const request = {
          header: jest.fn().mockReturnValue(undefined),
          ip: "127.0.0.1",
          socket: { remoteAddress: "127.0.0.1" },
        } as unknown as Request;

        await middleware?.(request, response, next);

        expect(next).toHaveBeenCalledTimes(1);
        expect(response.status).not.toHaveBeenCalled();
      } finally {
        jest.dontMock("rate-limiter-flexible");
        if (originalRedisUrl === undefined) {
          delete process.env.REDIS_URL;
        } else {
          process.env.REDIS_URL = originalRedisUrl;
        }
      }
    });
  });

  describe("RabbitMQ Outbox failure", () => {
    it("leaves an event PENDING when publish fails", async () => {
      const event = {
        id: "event-rabbit-down",
        aggregateId: "aggregate-1",
        eventType: "user.created",
        payload: { userId: "user-1" },
        status: "PENDING",
        createdAt: new Date("2026-11-02T20:00:00.000Z"),
      };
      const updateMany = jest.fn().mockResolvedValue({ count: 0 });
      const transaction = {
        $queryRaw: jest.fn().mockResolvedValue([event]),
        outboxEvent: { updateMany },
      };
      const prisma = {
        $transaction: jest.fn(async (callback: (tx: typeof transaction) => Promise<void>) =>
          callback(transaction),
        ),
      };
      const channel = {
        publish: jest.fn(() => {
          throw new Error("RabbitMQ unavailable");
        }),
      } as unknown as Channel;

      const worker = new OutboxPollerWorker(
        prisma as never,
        channel,
        { batchSize: 1, intervalMs: 1_000 },
      );

      await worker.poll();

      expect(transaction.$queryRaw).toHaveBeenCalled();
      expect(channel.publish).toHaveBeenCalledTimes(1);
      expect(updateMany).not.toHaveBeenCalled();
      expect(event.status).toBe("PENDING");
    });
  });
});
