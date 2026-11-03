#!/usr/bin/env bash
set -Eeuo pipefail

COMPOSE_FILE="docker-compose.prod.yml"
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

services=(api-gateway postgres rabbitmq prometheus grafana)
timeout_seconds="${DEPLOY_TIMEOUT_SECONDS:-180}"
ready=false

cleanup_on_error() {
  echo "Deployment failed. Recent service status:"
  docker compose -f "$COMPOSE_FILE" ps || true
}
trap cleanup_on_error ERR

echo "==> Building and starting production stack"
docker compose -f "$COMPOSE_FILE" up -d --build

echo "==> Waiting for core services (timeout: ${timeout_seconds}s)"
deadline=$((SECONDS + timeout_seconds))
while (( SECONDS < deadline )); do
  ready=true
  for service in "${services[@]}"; do
    status="$(docker compose -f "$COMPOSE_FILE" ps --format '{{.State}} {{.Health}}' "$service" 2>/dev/null || true)"
    if [[ "$status" != *"running"* ]]; then
      ready=false
    fi
    if [[ "$service" == "postgres" || "$service" == "rabbitmq" ]] && [[ "$status" != *"healthy"* ]]; then
      ready=false
    fi
  done
  curl -fsS --max-time 3 http://localhost:3002/health >/dev/null 2>&1 || ready=false
  curl -fsS --max-time 3 http://localhost:9090/-/healthy >/dev/null 2>&1 || ready=false
  curl -fsS --max-time 3 http://localhost:3000/api/health >/dev/null 2>&1 || ready=false
  if [[ "$ready" == true ]]; then break; fi
  sleep 5
done

if [[ "$ready" != true ]]; then
  echo "Timed out waiting for production services." >&2
  docker compose -f "$COMPOSE_FILE" ps
  exit 1
fi

echo "==> Production services"
docker compose -f "$COMPOSE_FILE" ps
echo
echo "API Gateway: http://localhost:3002/health"
echo "Grafana:     http://localhost:3000"
echo "Prometheus:  http://localhost:9090"
