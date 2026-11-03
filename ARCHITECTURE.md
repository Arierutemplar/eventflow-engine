# Eventflow Engine: arquitectura

## 1. Visión general

Eventflow Engine es un monorepo Node.js orientado al procesamiento asíncrono de eventos en tiempo real. La arquitectura separa la entrada HTTP, la persistencia transaccional, la publicación de eventos y el procesamiento independiente por consumidores.

### Estado de producción

El sistema implementa actualmente:

- **Transactional Outbox** con selección concurrente mediante `FOR UPDATE SKIP LOCKED`.
- Índice PostgreSQL compuesto `@@index([status, createdAt])` para acelerar la lectura FIFO de eventos `PENDING`.
- RabbitMQ con **Quorum Queues**, reintentos, Dead Letter Exchange (DLX) y replay operativo.
- Idempotencia distribuida atómica en Redis con `SET ... EX ... NX`, y fallback *fail-open* hacia la idempotencia persistida en PostgreSQL.
- Pruebas de caos para caídas temporales de Redis y RabbitMQ, manteniendo el procesamiento controlado y sin pérdida de eventos.
- Pruebas de carga y estrés con Autocannon (`npm run stress:api`, `npm run load:products`, `npm run load:test`) contra `POST /products`. El rendimiento depende del hardware y de la infraestructura local, por lo que no se fijan cifras de referencia.

### Stack tecnológico

- **Node.js 20+ y TypeScript** en modo estricto con módulos `NodeNext`.
- **npm workspaces** para compartir paquetes y gestionar las aplicaciones.
- **Fastify** para el API Gateway.
- **PostgreSQL 16** como almacenamiento principal.
- **Prisma ORM** para acceso tipado a PostgreSQL.
- **RabbitMQ** como broker de eventos, con exchange `topic`, reintentos y DLQ.
- **Redis** para caching, rate limiting y bloqueos distribuidos.
- **Prometheus** y `prom-client` para métricas.
- **Grafana** para dashboards y alertas.
- **Alertmanager** para enrutar y agrupar alertas de Prometheus.
- **Docker Compose** para el entorno local de infraestructura.

### Estructura principal

```text
./
├── apps/
│   ├── api-gateway/
│   ├── event-workers/
│   └── notification-service/
├── packages/
│   ├── common/
│   ├── database/
│   └── event-bus/
├── prometheus/
├── grafana/
├── alertmanager/
├── scripts/
├── tests/
├── docker-compose.yml
├── tsconfig.json
├── tsconfig.build.json
├── README.md
└── ARCHITECTURE.md
```

Todas las rutas y comandos de este documento son relativos a la raíz del repositorio, que contiene directamente los workspaces `apps/*` y `packages/*`.

## 2. Componentes y responsabilidades

### API Gateway

El API Gateway expone endpoints HTTP para la ingesta de comandos, por ejemplo:

- `POST /products`
- `GET /products`
- `GET /health`

Las peticiones siguen la separación:

```text
Router -> Controller -> Service -> Repository -> Prisma
```

La creación de un producto y su evento Outbox se ejecutan dentro de la misma transacción de PostgreSQL.

### Event Workers

`apps/event-workers` contiene dos responsabilidades operativas:

1. **Outbox Poller**: consulta mensajes pendientes y los publica en RabbitMQ.
2. **Event Consumer**: consume `events_queue`, ejecuta la lógica de negocio, aplica idempotencia y gestiona ACK, retry y DLQ.

El proceso principal expone métricas del Outbox en `4001`; el consumidor independiente expone métricas en `4002`.

### Notification Service

`apps/notification-service` es un consumidor independiente con:

- Cola propia: `notifications_queue`.
- Binding: `product.*`.
- ACK manual.
- Idempotencia propia en PostgreSQL.
- Reintentos y DLQ compartidos con la topología de RabbitMQ.
- Endpoint de métricas en `4003`.

Al usar una cola independiente, procesa en paralelo los mismos eventos que `event-workers`, sin competir por los mensajes de la cola original.

### Paquetes compartidos

- `packages/common`: tipos, DTOs, esquemas, errores, logging y Circuit Breaker.
- `packages/database`: cliente singleton de Prisma y schema de PostgreSQL.
- `packages/event-bus`: cliente RabbitMQ singleton, topología, publicación confirmada, ACK/NACK, reintentos y DLQ.

## 3. Patrón Transactional Outbox

El Gateway no publica directamente en RabbitMQ durante la petición HTTP. En su lugar, ejecuta una transacción PostgreSQL:

1. Crea el recurso de negocio, por ejemplo `Product`.
2. Crea un `OutboxMessage` con:
   - `eventType = "product.created"`.
   - Payload del evento.
   - `published = false`.
3. Confirma la transacción.

Esto evita el problema de dual write: el recurso y el evento quedan persistidos juntos o ninguno queda persistido.

El Outbox Poller consulta lotes de `OutboxEvent` con estado `PENDING`, ordenados por `createdAt`. La lectura usa una transacción y:

```sql
SELECT ...
FROM "OutboxEvent"
WHERE "status" = 'PENDING'
ORDER BY "createdAt" ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;
```

El índice `@@index([status, createdAt])` reduce el coste de localizar eventos pendientes. Para cada evento seleccionado:

1. Publica el evento en RabbitMQ.
2. Espera la confirmación del broker mediante `ConfirmChannel`.
3. Actualiza `status` de `PENDING` a `PUBLISHED`.

El bloqueo se mantiene durante la publicación y actualización para impedir que varios pollers procesen la misma fila. Si RabbitMQ no está disponible, la publicación falla explícitamente y el evento permanece `PENDING`, listo para el siguiente ciclo.

## 4. Topología de RabbitMQ

### Exchange principal

```text
eventflow.events
tipo: topic
```

Los `eventType` se usan como routing key. Ejemplo:

```text
product.created
```

### Colas principales

| Cola | Binding | Consumidor |
|---|---|---|
| `events_queue` | `#` o eventos generales | `event-workers` |
| `notifications_queue` | `product.*` | `notification-service` |

Cada consumidor tiene su propia cola para que RabbitMQ entregue una copia independiente del evento.

### Reintentos con TTL

Los errores recuperables se enrutan a colas de retry con TTL creciente:

| Cola | TTL |
|---|---:|
| `events_retry_queue` | 5 s |
| `events_retry_queue_10s` | 10 s |
| `events_retry_queue_20s` | 20 s |

Las colas de retry tienen `x-dead-letter-exchange=eventflow.events`. Cuando expira el TTL, RabbitMQ devuelve el mensaje al exchange principal. El consumidor inspecciona el header `x-death` para determinar el número de intentos.

Los headers originales se preservan al publicar el siguiente retry para que el contador no se reinicie.

### DLX y DLQ

```text
eventflow.events.dlx
events_queue_dlq
```

Después de agotar los reintentos, el consumidor publica el evento al Dead Letter Exchange con routing key `events_queue_dlq` y confirma el mensaje original. Los errores de enrutamiento no se reencolan indefinidamente: se rechazan con `nack(msg, false, false)`.

La topología quorum también define `events.exchange`, `events.dlx.exchange`, `user.events.queue` y `events.dlq`. El consumidor `dev:dlx` audita los mensajes muertos en PostgreSQL (`DeadLetterAudit`) con payload, headers, routing key, intentos y error. El script `replay:dlx` republica mensajes confirmados al exchange principal y solo hace ACK después de confirmar la publicación.

## 5. ACK, errores y reintentos

Los consumidores usan ACK manual (`noAck: false`):

- Procesamiento exitoso: `channel.ack(msg)`.
- Evento duplicado: ACK inmediato y seguro.
- Error recuperable dentro del límite: publicación en retry y ACK del mensaje original.
- Límite excedido: publicación en DLQ y ACK del mensaje original.
- Error al enrutar retry/DLQ: `channel.nack(msg, false, false)`.

Este flujo evita confirmar mensajes antes de que su resultado haya sido persistido o enrutado correctamente.

## 6. Idempotencia con PostgreSQL

El modelo `IdempotencyRecord` utiliza `key` como clave primaria:

```text
key       String @id
status    PROCESSING | COMPLETED | FAILED
response  Json?
```

Antes de ejecutar la lógica de negocio, cada consumidor intenta insertar su clave:

- `outbox:<eventId>` para el procesamiento del Outbox.
- `notification:<eventId>` para notificaciones.

El constraint único de PostgreSQL protege contra carreras concurrentes. Si la clave ya existe:

- `COMPLETED`: el evento se considera duplicado y se confirma con ACK.
- `PROCESSING`: se evita el procesamiento concurrente.
- `FAILED`: puede reclamarse según la lógica de reintento.

La idempotencia permite reentregas seguras de RabbitMQ y evita efectos secundarios duplicados.

### Idempotencia distribuida con Redis

Antes de ejecutar el handler, `event-workers` intenta registrar `eventId` con una operación atómica:

```text
SET eventflow:idempotency:<eventId> <token> EX <ttl> NX
```

- `OK`: el consumer adquiere el lock y continúa.
- Clave existente: el evento es duplicado, se registra con Pino y se confirma con ACK inmediato.
- Redis no disponible: se activa *fail-open* y el flujo continúa con la protección de PostgreSQL.
- Error de procesamiento: el lock se libera con un script Lua que valida el token, permitiendo el retry.

El TTL se configura con `REDIS_IDEMPOTENCY_TTL_SECONDS` y el prefijo con `REDIS_IDEMPOTENCY_PREFIX`.

## 7. Caos y resiliencia

La suite `tests/integration/chaos.test.ts` valida degradaciones controladas:

1. Una caída de Redis no bloquea la idempotencia del consumer ni el rate limiting del Gateway.
2. Una caída de RabbitMQ no marca eventos como publicados; el Outbox conserva el estado `PENDING`.

Esta combinación evita ACKs prematuros y garantiza que los eventos puedan reintentarse cuando la dependencia se recupere.

## 8. Escalabilidad y procesamiento paralelo

La escalabilidad horizontal se consigue ejecutando varias instancias de cada consumidor:

```text
eventflow.events
├── events_queue        -> event-workers (N consumidores)
└── notifications_queue -> notification-service (M consumidores)
```

Dentro de una misma cola, RabbitMQ distribuye los mensajes entre consumidores. Entre colas distintas, cada servicio recibe su propia copia del evento.

Recomendaciones operativas:

- Aumentar réplicas de `event-workers` para el procesamiento principal.
- Aumentar réplicas de `notification-service` de forma independiente.
- Ajustar `prefetch` según CPU, latencia y tamaño de los mensajes.
- Mantener handlers idempotentes.
- Usar lotes controlados en el Outbox Poller.
- Monitorizar profundidad de colas, retries, DLQ y latencia.

## 9. Pools de Prisma y PostgreSQL

Prisma obtiene el pool desde `DATABASE_URL`. La configuración recomendada para el entorno local y cada microservicio es:

```env
DATABASE_URL="postgresql://eventuser:eventpassword@localhost:5432/eventflow_db?schema=public&connection_limit=5&pool_timeout=10"
```

`connection_limit=5` limita las conexiones simultáneas por instancia de Prisma y `pool_timeout=10` evita esperas indefinidas al obtener una conexión.

Con varias réplicas, el presupuesto debe calcularse antes de aumentar instancias:

```text
conexiones totales =
  conexiones administrativas
  + réplicas_gateway * connection_limit
  + réplicas_workers * connection_limit
  + réplicas_notifications * connection_limit
```

PostgreSQL se configura con un límite superior a ese presupuesto. En Docker Compose se puede usar:

```yaml
postgres:
  command: ["postgres", "-c", "max_connections=100"]
```

El límite debe considerar conexiones de migraciones, Prisma Studio, health checks y administración. No se debe aumentar `max_connections` sin dimensionar memoria y carga.

Cada proceso usa un cliente Prisma singleton y debe cerrar la conexión durante el shutdown cuando el proceso se ejecuta como servicio independiente.

## 10. Circuit Breaker

`packages/common` contiene un Circuit Breaker reutilizable para proteger dependencias críticas, actualmente aplicado al acceso de productos del API Gateway.

Configuración actual:

| Parámetro | Valor |
|---|---:|
| Volumen mínimo | 5 solicitudes |
| Umbral de errores | 50% |
| Timeout de operación | 5 s |
| Reset timeout | 10 s |

Estados:

- **Closed**: las solicitudes se ejecutan normalmente.
- **Open**: se rechazan llamadas nuevas y se evita seguir presionando la dependencia.
- **Half-open**: tras el reset timeout, se permite una solicitud de prueba.
- Éxito en la prueba: vuelve a `closed`.
- Fallo en la prueba: vuelve a `open`.

El fallback del listado de productos devuelve una lista vacía cuando la base de datos no está disponible. Para operaciones de escritura se devuelve un error explícito, evitando reportar una creación como exitosa cuando no fue persistida.

El Gateway publica:

```text
eventflow_circuit_breaker_state{service="api-gateway-database",state="closed|open|half-open"}
```

El estado `open` activa la alerta `EventflowCircuitBreakerOpen`.

## 11. Observabilidad

### Métricas de aplicación

| Servicio | Endpoint |
|---|---|
| Outbox worker | `http://localhost:4001/metrics` |
| Event consumer | `http://localhost:4002/metrics` |
| Notification service | `http://localhost:4003/metrics` |
| API Gateway | `http://localhost:4004/metrics` |

Métricas principales:

- Eventos Outbox publicados y fallidos.
- Eventos procesados exitosamente.
- Duplicados detectados.
- Errores de procesamiento.
- Reintentos.
- Eventos enviados a DLQ.
- Mensajes Outbox pendientes.
- Estado del Circuit Breaker.

### Prometheus

Prometheus recolecta los endpoints de las aplicaciones y RabbitMQ:

```text
prometheus:9090
rabbitmq:15692
```

La configuración está en `prometheus/prometheus.yml`. Las reglas se cargan desde `prometheus/alerts/`.

### Grafana

Grafana se provisiona automáticamente mediante:

- `grafana/provisioning/datasources/datasources.yaml`
- `grafana/provisioning/dashboards/dashboards.yaml`
- `grafana/dashboards/eventflow-performance.json`

El dashboard visualiza:

- Tasa de eventos procesados.
- Errores.
- Duplicados.
- Reintentos.
- Mensajes en DLQ.
- Mensajes listos en colas RabbitMQ.

### Alertmanager

Alertmanager recibe las alertas de Prometheus en `alertmanager:9093`, las agrupa por alerta y componente y aplica los intervalos de repetición definidos en `alertmanager/alertmanager.yml`.

Alertas configuradas:

- Circuit Breaker abierto.
- Tasa de errores superior al 10%.
- DLQ con mensajes.
- Colas de retry con más de 100 mensajes listos.

## 12. Operación local y scripts

Levantar infraestructura (en una terminal desde la raíz):

```bash
docker compose up -d postgres redis rabbitmq prometheus alertmanager grafana
```

Para levantar solo PostgreSQL, Redis y RabbitMQ: `npm run docker:db`.

Preparar la base de datos y compilar:

```bash
npm run db:generate
npm run db:push
npm run build
```

Levantar aplicaciones en desarrollo (una terminal por proceso):

```bash
npm run dev:gateway
npm run dev:poller
npm run dev:worker
npm run dev:dlx
npm run dev:notifications
```

Para ejecutar el API Gateway ya compilado: `npm run build && npm start`.

Validar configuración:

```bash
docker compose config --quiet
```

### Scripts principales

| Comando | Propósito |
|---|---|
| `npm run build` | Compila todos los workspaces TypeScript (`tsconfig.build.json`). |
| `npm run clean` | Elimina la salida compilada `dist/`. |
| `npm run db:generate` | Regenera el cliente Prisma. |
| `npm run db:migrate` | Ejecuta migraciones Prisma. |
| `npm run db:push` | Sincroniza el schema Prisma con la base de datos configurada. |
| `npm run load:products` | Prueba de carga a ritmo constante contra `POST /products`. |
| `npm run load:test` | Prueba de carga de saturación contra `POST /products`. |
| `npm run dev:gateway` | Ejecuta el API Gateway con watch y OpenTelemetry. |
| `npm run dev:poller` | Ejecuta el Outbox Poller con watch. |
| `npm run dev:worker` | Ejecuta el consumer principal con watch. |
| `npm run dev:dlx` | Ejecuta el consumer de la DLQ con watch. |
| `npm run dev:notifications` | Ejecuta el Notification Service con watch. |
| `npm run docker:db` | Levanta PostgreSQL, Redis y RabbitMQ con Docker Compose. |
| `npm run db:seed` | Inserta un evento de prueba en el Outbox. |
| `npm run db:studio` | Abre Prisma Studio. |
| `npm start` | Ejecuta el API Gateway compilado (`dist/`). |
| `npm run stress:api` | Ejecuta Autocannon contra `POST /products`. |
| `npm run replay:dlx` | Republica mensajes de la DLQ; admite `REPLAY_LIMIT`. |
| `npm run test` | Ejecuta la suite Jest unitaria. |
| `npm run test:e2e` | Ejecuta pruebas E2E con PostgreSQL y RabbitMQ. |
| `npm run test:integration` | Prepara el schema de integración y ejecuta pruebas de integración. |

Detener servicios de aplicación con `Ctrl+C` o finalizando sus procesos. Los contenedores de infraestructura pueden mantenerse activos como entorno local.
