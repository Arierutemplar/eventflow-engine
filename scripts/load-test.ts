import autocannon from "autocannon";

const targetUrl = process.env.TARGET_URL ?? "http://localhost:3002/products";
const productPayload = JSON.stringify({
  name: "Load Test Product",
  price: 3500,
  status: "ACTIVE",
});

async function main(): Promise<void> {
  const result = await new Promise<autocannon.Result>((resolve, reject) => {
    const instance = autocannon(
      {
        url: targetUrl,
        method: "POST",
        connections: 100,
        pipelining: 10,
        duration: 30,
        headers: {
          "content-type": "application/json",
        },
        body: productPayload,
      },
      (error, completedResult) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(completedResult);
      },
    );

    autocannon.track(instance, { renderProgressBar: true });
  });

  console.log("\n\n=== Load Test Summary ===");
  console.log(`Target: ${targetUrl}`);
  console.log(`Throughput promedio: ${result.requests.average.toFixed(2)} req/sec`);
  console.log(`Latencia media: ${result.latency.average.toFixed(2)} ms`);
  console.log(`Peticiones completadas: ${result.requests.total}`);
  console.log(`Errores: ${result.errors}`);
  console.log(`Timeouts: ${result.timeouts}`);
}

main().catch((error: unknown) => {
  console.error("[load-test] Error ejecutando la prueba:", error);
  process.exitCode = 1;
});
