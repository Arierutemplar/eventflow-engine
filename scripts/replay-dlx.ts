import amqp from "amqplib";
import {
  EVENTS_DLQ,
  EVENTS_DLX_EXCHANGE,
  EVENTS_EXCHANGE,
} from "@eventflow/event-bus";

const rabbitUrl = process.env.RABBITMQ_URL ?? "amqp://localhost:5672";
const limit = Number(process.env.REPLAY_LIMIT ?? 100);

if (!Number.isInteger(limit) || limit <= 0) {
  throw new Error("REPLAY_LIMIT debe ser un entero mayor que cero");
}

async function main(): Promise<void> {
  const connection = await amqp.connect(rabbitUrl);
  const channel = await connection.createConfirmChannel();
  try {
    await channel.assertExchange(EVENTS_EXCHANGE, "topic", { durable: true });
    await channel.assertExchange(EVENTS_DLX_EXCHANGE, "topic", { durable: true });
    await channel.assertQueue(EVENTS_DLQ, {
      durable: true,
      arguments: { "x-queue-type": "quorum" },
    });
    await channel.bindQueue(EVENTS_DLQ, EVENTS_DLX_EXCHANGE, "#");

    let replayed = 0;
    while (replayed < limit) {
      const message = await channel.get(EVENTS_DLQ, { noAck: false });
      if (!message) break;

      const headers: Record<string, unknown> = {
        ...(message.properties.headers ?? {}),
      };
      const routingKey =
        typeof headers["x-original-routing-key"] === "string"
          ? headers["x-original-routing-key"]
          : message.fields.routingKey;
      const replayHeaders: Record<string, unknown> = {
        ...headers,
        "x-replayed-from": EVENTS_DLQ,
      };
      delete replayHeaders["x-retry-count"];
      delete replayHeaders["x-death"];

      const accepted = channel.publish(
        EVENTS_EXCHANGE,
        routingKey,
        message.content,
        {
          persistent: true,
          contentType: message.properties.contentType ?? "application/json",
          headers: replayHeaders,
        },
      );
      if (!accepted) await new Promise<void>((resolve) => channel.once("drain", resolve));
      await channel.waitForConfirms();
      channel.ack(message);
      replayed += 1;
    }
    console.log(`[dlx-replay] ${replayed} mensaje(s) republicado(s).`);
  } finally {
    await channel.close();
    await connection.close();
  }
}

main().catch((error: unknown) => {
  console.error("[dlx-replay] Error republicando mensajes:", error);
  process.exitCode = 1;
});
