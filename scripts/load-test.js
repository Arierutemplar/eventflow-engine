import autocannon from "autocannon";

autocannon({
  url: process.env.TARGET_URL ?? "http://localhost:3002/health",
  connections: Number(process.env.CONNECTIONS ?? 10),
  duration: Number(process.env.DURATION ?? 10),
});