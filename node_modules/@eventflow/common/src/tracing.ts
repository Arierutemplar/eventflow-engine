import {
  context,
  propagation,
  trace,
  type Context,
  type Span,
} from "@opentelemetry/api";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { NodeSDK } from "@opentelemetry/sdk-node";

const serviceName = process.env.SERVICE_NAME ?? "eventflow-service";
const endpoint =
  process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??
  "http://localhost:4318/v1/traces";

const sdk = new NodeSDK({
  serviceName,
  traceExporter: new OTLPTraceExporter({ url: endpoint }),
  instrumentations: [getNodeAutoInstrumentations()],
});

let started = false;
let shutdownRegistered = false;

export function startTracing(): void {
  if (started) return;
  sdk.start();
  started = true;

  if (!shutdownRegistered) {
    shutdownRegistered = true;
    process.once("SIGTERM", () => {
      void shutdownTracing();
    });
  }
}

export async function shutdownTracing(): Promise<void> {
  if (!started) return;
  await sdk.shutdown();
  started = false;
}

export function injectTraceparent<T extends Record<string, unknown>>(
  payload: T,
): T & { metadata: { traceparent?: string } } {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return {
    ...payload,
    metadata: {
      ...(isRecord(payload.metadata) ? payload.metadata : {}),
      ...(carrier.traceparent ? { traceparent: carrier.traceparent } : {}),
    },
  };
}

export function extractTraceContext(traceparent: unknown): Context {
  if (typeof traceparent !== "string" || traceparent.length === 0) {
    return context.active();
  }
  return propagation.extract(context.active(), { traceparent });
}

export function startConsumerSpan(name: string, parentContext: Context): Span {
  return trace.getTracer(serviceName).startSpan(name, undefined, parentContext);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
