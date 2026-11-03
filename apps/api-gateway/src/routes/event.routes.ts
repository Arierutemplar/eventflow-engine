import type { FastifyInstance } from "fastify";
import { validateEvent } from "../controllers/event.controller.js";

export async function registerEventRoutes(app: FastifyInstance): Promise<void> {
  app.post("/events", async (request, reply) => {
    const event = validateEvent(request.body);
    return reply.code(202).send({ accepted: true, eventId: event.id });
  });
}