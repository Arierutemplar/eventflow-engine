-- Schema aligned with packages/database/prisma/schema.prisma.
-- Applied only on a fresh PostgreSQL volume.

CREATE TABLE IF NOT EXISTS "Order" (
    "id" TEXT NOT NULL,
    "customerId" TEXT NOT NULL,
    "total" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "Order_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "Product" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "price" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'ACTIVE',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "Product_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "Event" (
    "id" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "Event_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "OutboxMessage" (
    "id" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "published" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "sentAt" TIMESTAMP(3),
    CONSTRAINT "OutboxMessage_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "IdempotencyRecord" (
    "id" TEXT NOT NULL,
    "eventId" TEXT,
    "key" TEXT,
    "status" TEXT NOT NULL DEFAULT 'PROCESSING',
    "result" JSONB,
    "response" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "IdempotencyRecord_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "OutboxEvent" (
    "id" TEXT NOT NULL,
    "aggregateId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "OutboxEvent_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "DeadLetterAudit" (
    "id" TEXT NOT NULL,
    "eventId" TEXT,
    "eventType" TEXT,
    "routingKey" TEXT NOT NULL,
    "sourceQueue" TEXT NOT NULL,
    "headers" JSONB,
    "payload" JSONB NOT NULL,
    "errorMessage" TEXT,
    "failedAttempts" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "DeadLetterAudit_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "Order_customerId_createdAt_idx" ON "Order"("customerId", "createdAt");
CREATE INDEX IF NOT EXISTS "Order_status_createdAt_idx" ON "Order"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "Product_status_createdAt_idx" ON "Product"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "Product_name_idx" ON "Product"("name");
CREATE INDEX IF NOT EXISTS "Event_status_createdAt_idx" ON "Event"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "Event_type_createdAt_idx" ON "Event"("type", "createdAt");
CREATE UNIQUE INDEX IF NOT EXISTS "IdempotencyRecord_eventId_key" ON "IdempotencyRecord"("eventId");
CREATE UNIQUE INDEX IF NOT EXISTS "IdempotencyRecord_key_key" ON "IdempotencyRecord"("key");
CREATE INDEX IF NOT EXISTS "IdempotencyRecord_eventId_idx" ON "IdempotencyRecord"("eventId");
CREATE INDEX IF NOT EXISTS "IdempotencyRecord_status_updatedAt_idx" ON "IdempotencyRecord"("status", "updatedAt");
CREATE INDEX IF NOT EXISTS "OutboxMessage_published_createdAt_idx" ON "OutboxMessage"("published", "createdAt");
CREATE INDEX IF NOT EXISTS "OutboxMessage_eventType_createdAt_idx" ON "OutboxMessage"("eventType", "createdAt");
CREATE INDEX IF NOT EXISTS "OutboxEvent_status_createdAt_idx" ON "OutboxEvent"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "OutboxEvent_aggregateId_createdAt_idx" ON "OutboxEvent"("aggregateId", "createdAt");
CREATE INDEX IF NOT EXISTS "OutboxEvent_eventType_createdAt_idx" ON "OutboxEvent"("eventType", "createdAt");
CREATE INDEX IF NOT EXISTS "DeadLetterAudit_eventId_createdAt_idx" ON "DeadLetterAudit"("eventId", "createdAt");
CREATE INDEX IF NOT EXISTS "DeadLetterAudit_eventType_createdAt_idx" ON "DeadLetterAudit"("eventType", "createdAt");
CREATE INDEX IF NOT EXISTS "DeadLetterAudit_createdAt_idx" ON "DeadLetterAudit"("createdAt");
