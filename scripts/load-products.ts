import autocannon from "autocannon";

const targetUrl = process.env.TARGET_URL ?? "http://localhost:3002/products";
const duration = Number(process.env.DURATION ?? 30);
const rate = Number(process.env.RATE ?? 50);
const connections = Number(process.env.CONNECTIONS ?? 25);

if (
  !Number.isInteger(duration) ||
  duration <= 0 ||
  !Number.isInteger(rate) ||
  rate <= 0 ||
  !Number.isInteger(connections) ||
  connections <= 0
) {
  throw new Error("DURATION, RATE y CONNECTIONS deben ser enteros mayores que cero");
}

async function main(): Promise<void> {
  console.log(
    `[load-test] POST ${targetUrl} | ${rate} req/s | ${duration}s | ${connections} conexiones`,
  );

  const result = await autocannon({
    url: targetUrl,
    method: "POST",
    connections,
    duration,
    overallRate: rate,
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      name: "Load Test Product",
      price: 3500,
      status: "ACTIVE",
    }),
  });

  console.log(
    JSON.stringify(
      {
        requests: result.requests,
        latency: result.latency,
        errors: result.errors,
        timeouts: result.timeouts,
        non2xx: result.non2xx,
        statusCodeStats: result.statusCodeStats,
      },
      null,
      2,
    ),
  );

  process.exitCode =
    result.errors > 0 || result.timeouts > 0 || result.non2xx > 0 ? 1 : 0;
}

main().catch((error: unknown) => {
  console.error("[load-test] Error ejecutando la prueba:", error);
  process.exitCode = 1;
});
