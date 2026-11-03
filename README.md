# EventFlow Engine

Plataforma de procesamiento de eventos **orientada a eventos (Event-Driven Architecture)** construida como monorepo TypeScript. Recibe peticiones HTTP, persiste el cambio de negocio y el evento en una misma transacción (patrón **Transactional Outbox**), y lo distribuye de forma asíncrona y fiable a través de **RabbitMQ** hacia consumidores idempotentes, con reintentos, *dead-letter queues*, métricas y trazas distribuidas.

## Puntos fuertes

| Característica | Cómo se implementa |
|---|---|
| **Entrega fiable (patrón Outbox)** | El evento se guarda en PostgreSQL junto al cambio de negocio en una sola transacción; un *Outbox Poller* lo publica en RabbitMQ después. Sin *dual writes* ni eventos perdidos. |
| **Idempotencia** | Registros de idempotencia en PostgreSQL y, opcionalmente, en Redis (TTL configurable). Un mismo evento entregado varias veces se procesa una sola vez. |
| **Procesamiento asíncrono** | API Gateway, Outbox Poller, consumidores y servicio de notificaciones son procesos independientes desacoplados por el broker. |
| **Resiliencia** | Reintentos con *backoff* mediante colas TTL (5 s, 10 s, 20 s), DLX/DLQ con auditoría y *replay*, *Circuit Breaker* hacia la base de datos y *rate limiting* distribuido con Redis. |
| **Escalabilidad** | Servicios sin estado desacoplados por el broker: los consumidores se escalan horizontalmente como *competing consumers* sobre la misma cola. |
| **Observabilidad** | Métricas Prometheus (`prom-client`), dashboards Grafana, alertas con Alertmanager, trazas OpenTelemetry y logs JSON estructurados (Pino) con `trace_id`/`span_id`. |
| **Calidad** | Pruebas unitarias, de integración, E2E y de caos con Jest; pruebas de carga y estrés con Autocannon. |

## Casos de uso

- **Integración fiable entre microservicios**: publicar eventos de dominio (`product.created`, `order.created`…) sin perderlos aunque el broker o el servicio caigan.
- **Procesamiento en segundo plano**: notificaciones, auditoría, sincronización con sistemas externos o análisis sin bloquear la respuesta HTTP.
- **Sistemas que toleran duplicados**: entrega *at-least-once* con idempotencia, adecuada para pagos, pedidos o inventario.
- **Referencia técnica**: base para estudiar Outbox, DLQ, reintentos, circuit breakers y observabilidad de extremo a extremo.

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje y runtime | Node.js ≥ 20, TypeScript 5 (ESM, `NodeNext`) |
| API | Fastify 5 (gateway); middleware de *rate limiting* con `rate-limiter-flexible` |
| Persistencia | PostgreSQL 16 + Prisma ORM 6 |
| Mensajería | RabbitMQ 3 (`amqplib`), exchange `topic`, colas de reintento con TTL y DLX |
| Caché / locks / límites | Redis 7 (`ioredis`) |
| Validación | Zod |
| Observabilidad | Prometheus, Grafana, Alertmanager, OpenTelemetry, Pino |
| Contenedores | Docker, Docker Compose (local y producción) |
| Pruebas | Jest, ts-jest, Autocannon |
| CI | GitHub Actions |

## Estructura del monorepo

El repositorio es plano: todos los workspaces cuelgan directamente de la raíz.

```text
.
├── apps/
│   ├── api-gateway/            # API HTTP (Fastify): /events, /products, /health, /metrics
│   ├── event-workers/          # Outbox Poller, consumidor principal y consumidor DLX
│   └── notification-service/   # Consumidor independiente de notificaciones
├── packages/
│   ├── common/                 # Logger, trazas, esquemas Zod, errores, circuit breaker
│   ├── database/               # Cliente Prisma y schema PostgreSQL
│   └── event-bus/              # Cliente RabbitMQ y topología
├── scripts/                    # seed, replay de DLQ, pruebas de carga y estrés
├── tests/integration/          # Integración, E2E y caos
├── prometheus/ grafana/ alertmanager/   # Configuración de observabilidad
├── docker-compose.yml          # Infraestructura local
├── docker-compose.prod.yml     # Stack de producción
├── tsconfig.json               # Type-check completo (incluye tests y scripts)
└── tsconfig.build.json         # Compilación de producción (solo apps/ y packages/)
```

Workspaces npm: `apps/*` y `packages/*`. Los paquetes internos se importan por su nombre (`@eventflow/common`, `@eventflow/database`, `@eventflow/event-bus`) y solo exponen su punto de entrada raíz.

## Flujo de un evento

```text
Cliente ──HTTP──▶ API Gateway ──(transacción)──▶ PostgreSQL [negocio + Outbox]
                                                      │
                                         Outbox Poller (event-workers)
                                                      ▼
                                      RabbitMQ  (exchange topic)
                          ┌───────────────┴────────────────┐
                    Consumidor principal          Notification Service
                  (idempotente + reintentos)        (idempotente)
                          │ fallos definitivos
                          ▼
                     DLX ▶ DLQ ▶ auditoría / replay
```

Detalle completo en [ARCHITECTURE.md](./ARCHITECTURE.md).

## Instalación y ejecución local

### Requisitos

- Node.js **20 o superior** y npm
- Docker y Docker Compose

> En PowerShell de Windows, si la política de ejecución bloquea `npm`, usa `npm.cmd` o Git Bash.

### 1. Instalar dependencias

```bash
git clone https://github.com/Arierutemplar/eventflow-engine.git
cd eventflow-engine
npm install
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env        # PowerShell: Copy-Item .env.example .env
```

Los valores por defecto coinciden con `docker-compose.yml`. Variables principales:

| Variable | Descripción | Por defecto |
|---|---|---|
| `DATABASE_URL` | Conexión PostgreSQL (la lee Prisma) | `postgresql://eventuser:eventpassword@localhost:5432/eventflow_db?schema=public` |
| `RABBITMQ_URL` | Conexión AMQP | `amqp://guest:guest@localhost:5672` |
| `REDIS_URL` | Redis para *rate limiting* e idempotencia distribuida | `redis://localhost:6379` |
| `API_PORT` | Puerto del API Gateway | `3002` |
| `LOG_LEVEL` | Nivel de logs Pino | `info` |
| `SERVICE_NAME` / `OTEL_EXPORTER_OTLP_ENDPOINT` | Trazas OpenTelemetry | ver `.env.example` |
| `RATE_LIMIT_POINTS` / `RATE_LIMIT_DURATION_SECONDS` | Límite de peticiones | `100` / `60` |

Las pruebas leen `.env.test` (base de datos `eventflow_test`). **No subas `.env` ni `.env.test` al repositorio.**

### 3. Levantar la infraestructura

```bash
npm run docker:db           # PostgreSQL, Redis y RabbitMQ
```

Para añadir Prometheus, Grafana y Alertmanager: `docker compose up -d`.

### 4. Generar el cliente Prisma y preparar la base de datos

```bash
npm run db:generate         # genera el cliente Prisma
npm run db:push             # sincroniza el schema con la base de datos
```

`npm run db:migrate` crea y aplica una migración de desarrollo. El historial en `packages/database/prisma/migrations` no cubre todavía todos los modelos del schema (por ejemplo `Product`, `OutboxEvent` o `DeadLetterAudit`), por lo que `db:push` es la vía recomendada en local hasta consolidar las migraciones.

### 5. Compilar

```bash
npm run build               # tsc -p tsconfig.build.json → dist/
```

### 6. Ejecutar los servicios

En terminales separadas:

```bash
npm run dev:gateway         # API Gateway          → http://localhost:3002
npm run dev:poller          # Outbox Poller        (publica Outbox → RabbitMQ)
npm run dev:worker          # Consumidor principal (idempotente, con reintentos)
npm run dev:dlx             # Consumidor DLX       (auditoría de mensajes fallidos)
npm run dev:notifications   # Servicio de notificaciones
```

Para ejecutar el gateway compilado: `npm run build && npm start`.

### 7. Probar la API

```bash
curl http://localhost:3002/health

curl -X POST http://localhost:3002/products \
  -H "content-type: application/json" \
  -d '{"name":"Teclado","price":2500,"status":"ACTIVE"}'
```

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/health` | Estado del servicio |
| `GET` | `/metrics` | Métricas Prometheus |
| `POST` | `/events` | Valida un evento y responde `202 Accepted` |
| `POST` | `/products` | Crea un producto y su evento Outbox (`price` entero ≥ 0) |
| `GET` | `/products` | Lista productos |

Puertos locales: Gateway `3002`, RabbitMQ UI `15672`, Prometheus `9090`, Grafana `3001`, Alertmanager `9093`. Métricas por proceso: gateway `4004`, Outbox Poller `4001`, consumidor `4002`, notificaciones `4003`.

## Pruebas

```bash
npm test                    # Jest (todas las suites; las de integración usan DATABASE_URL, por eso
                            # conviene ejecutar antes `npm run db:push` o usar test:integration)
npm run test:integration    # sincroniza el schema de pruebas y ejecuta tests/integration
npm run test:e2e            # E2E con PostgreSQL y RabbitMQ reales (.env.test)
```

Las pruebas de integración y E2E requieren la infraestructura levantada (paso 3).

## Scripts de carga, estrés y operación

| Comando | Propósito |
|---|---|
| `npm run stress:api` | Autocannon contra `POST /products` (`TARGET_URL`, `PORT`, `CONNECTIONS`, `DURATION`) |
| `npm run load:products` | Carga a ritmo constante (`RATE`, `DURATION`, `CONNECTIONS`); falla si hay errores o respuestas non-2xx |
| `npm run load:test` | Prueba de saturación con *pipelining* |
| `npm run replay:dlx` | Republica mensajes de la DLQ (`REPLAY_LIMIT`) |
| `npm run db:seed` | Inserta un evento de prueba en el Outbox |
| `npm run db:studio` | Prisma Studio |
| `npm run clean` | Elimina `dist/` |

## Producción

`docker-compose.prod.yml` construye las imágenes de `api-gateway` y `event-workers` (Dockerfile multietapa, usuario no root) junto a PostgreSQL, RabbitMQ, Prometheus y Grafana. Las credenciales se leen de `.env` (variables `POSTGRES_*`, `RABBITMQ_*`, `GRAFANA_*`).

```bash
./deploy-prod.sh            # Linux/macOS
./deploy-prod.ps1           # Windows PowerShell
```

El pipeline de [GitHub Actions](./.github/workflows/ci.yml) instala dependencias, genera el cliente Prisma, comprueba tipos, compila y ejecuta las pruebas de integración contra PostgreSQL y RabbitMQ.

## Documentación adicional

- [ARCHITECTURE.md](./ARCHITECTURE.md): componentes, topología RabbitMQ, idempotencia, circuit breaker, pools de Prisma y observabilidad.
