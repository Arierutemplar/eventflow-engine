import amqp from "amqplib";
import type { ChannelModel, ConfirmChannel, ConsumeMessage } from "amqplib";

export const EVENT_EXCHANGE = "eventflow.events";
export const MAIN_EVENT_QUEUE = "events_queue";
export const DEAD_LETTER_EXCHANGE = "eventflow.events.dlx";
export const DEAD_LETTER_QUEUE = "events_queue_dlq";
export const RETRY_QUEUES = [
  { name: "events_retry_queue", ttlMs: 5_000 },
  { name: "events_retry_queue_10s", ttlMs: 10_000 },
  { name: "events_retry_queue_20s", ttlMs: 20_000 },
] as const;
export const MAX_RETRIES = RETRY_QUEUES.length;

export class RabbitMQClient {
  private static instance: RabbitMQClient | null = null;
  private connection: ChannelModel | null = null;
  private channel: ConfirmChannel | null = null;
  private reconnectPromise: Promise<void> | null = null;
  private shouldReconnect = true;

  constructor(private readonly url: string) {}

  static getInstance(
    url = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672",
  ): RabbitMQClient {
    if (!RabbitMQClient.instance) {
      RabbitMQClient.instance = new RabbitMQClient(url);
    }
    return RabbitMQClient.instance;
  }

  async connect(): Promise<void> {
    if (this.channel) return;
    if (this.reconnectPromise) return this.reconnectPromise;
    this.reconnectPromise = this.connectWithRetry();
    try {
      await this.reconnectPromise;
    } finally {
      this.reconnectPromise = null;
    }
  }

  private async connectWithRetry(): Promise<void> {
    let delayMs = 1_000;
    while (this.shouldReconnect) {
      try {
        const connection = await amqp.connect(this.url);
        const channel = await connection.createConfirmChannel();
        await this.assertTopology(channel);
        connection.on("error", (error) =>
          console.error("[!] Error de conexión RabbitMQ:", error),
        );
        connection.on("close", () => {
          if (this.connection === connection) {
            this.connection = null;
            this.channel = null;
          }
          if (this.shouldReconnect) void this.connect();
        });
        this.connection = connection;
        this.channel = channel;
        console.log("[x] Conectado a RabbitMQ y topología declarada.");
        return;
      } catch (error) {
        console.error(
          `[!] Error conectando a RabbitMQ; reintentando en ${delayMs} ms:`,
          error,
        );
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        delayMs = Math.min(delayMs * 2, 30_000);
      }
    }
  }

  private async assertTopology(channel: ConfirmChannel): Promise<void> {
    await channel.assertExchange(EVENT_EXCHANGE, "topic", { durable: true });
    await channel.assertExchange(DEAD_LETTER_EXCHANGE, "topic", {
      durable: true,
    });
    await channel.assertQueue(MAIN_EVENT_QUEUE, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
      },
    });
    await channel.bindQueue(MAIN_EVENT_QUEUE, EVENT_EXCHANGE, "#");
    await channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
    await channel.bindQueue(
      DEAD_LETTER_QUEUE,
      DEAD_LETTER_EXCHANGE,
      "#",
    );

    for (const retryQueue of RETRY_QUEUES) {
      await channel.assertQueue(retryQueue.name, {
        durable: true,
        arguments: {
          "x-message-ttl": retryQueue.ttlMs,
          "x-dead-letter-exchange": EVENT_EXCHANGE,
        },
      });
    }
  }

  async publishEvent(routingKey: string, message: unknown): Promise<void> {
    const channel = await this.getChannel();
    await this.publishAndConfirm(
      channel,
      EVENT_EXCHANGE,
      routingKey,
      message,
    );
  }

  async publishToRetry(
    retryNumber: number,
    routingKey: string,
    message: unknown,
    headers?: Record<string, unknown>,
  ): Promise<string> {
    const retryQueue = RETRY_QUEUES[retryNumber - 1];
    if (!retryQueue) {
      throw new Error(`No existe cola de reintento para el intento ${retryNumber}`);
    }

    const channel = await this.getChannel();
    await this.publishAndConfirm(
      channel,
      "",
      retryQueue.name,
      message,
      routingKey,
      headers,
    );
    return retryQueue.name;
  }

  async publishToDeadLetter(
    routingKey: string,
    message: unknown,
    headers?: Record<string, unknown>,
  ): Promise<void> {
    const channel = await this.getChannel();
    await this.publishAndConfirm(
      channel,
      DEAD_LETTER_EXCHANGE,
      DEAD_LETTER_QUEUE,
      message,
      routingKey,
      headers,
    );
  }

  async publishToQueue(queueName: string, message: unknown): Promise<boolean> {
    const channel = await this.getChannel();
    await channel.assertQueue(queueName, { durable: true });
    await this.publishAndConfirm(channel, "", queueName, message);
    return true;
  }

  async consume(
    queueName: string,
    onMessage: (message: ConsumeMessage | null) => Promise<void> | void,
    routingKey = "#",
  ): Promise<void> {
    const channel = await this.getChannel();
    await channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
      },
    });
    await channel.bindQueue(queueName, EVENT_EXCHANGE, routingKey);
    await channel.consume(
      queueName,
      (message) => {
        void onMessage(message);
      },
      { noAck: false },
    );
  }

  ack(message: ConsumeMessage): void {
    this.getRequiredChannel().ack(message);
  }

  nack(message: ConsumeMessage, allUpTo = false, requeue = false): void {
    this.getRequiredChannel().nack(message, allUpTo, requeue);
  }

  async close(): Promise<void> {
    this.shouldReconnect = false;
    const channel = this.channel;
    const connection = this.connection;
    this.channel = null;
    this.connection = null;
    if (channel) await channel.close();
    if (connection) await connection.close();
  }

  private async publishAndConfirm(
    channel: ConfirmChannel,
    exchange: string,
    routingKey: string,
    message: unknown,
    logRoutingKey = routingKey,
    headers?: Record<string, unknown>,
  ): Promise<void> {
    const accepted = channel.publish(
      exchange,
      routingKey,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
        headers: {
          ...headers,
          "x-original-routing-key": logRoutingKey,
        },
      },
    );
    if (!accepted) {
      await new Promise<void>((resolve) => channel.once("drain", resolve));
    }
    await channel.waitForConfirms();
  }

  private async getChannel(): Promise<ConfirmChannel> {
    await this.connect();
    return this.getRequiredChannel();
  }

  private getRequiredChannel(): ConfirmChannel {
    if (!this.channel) {
      throw new Error("El canal de RabbitMQ no está inicializado.");
    }
    return this.channel;
  }
}
