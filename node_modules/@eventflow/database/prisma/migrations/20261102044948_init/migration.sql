/*
  Warnings:

  - Added the required column `updatedAt` to the `IdempotencyRecord` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "IdempotencyRecord" ADD COLUMN     "status" TEXT NOT NULL DEFAULT 'PROCESSING',
ADD COLUMN     "updatedAt" TIMESTAMP(3) NOT NULL,
ALTER COLUMN "response" DROP NOT NULL;

-- CreateTable
CREATE TABLE "Order" (
    "id" TEXT NOT NULL,
    "customerId" TEXT NOT NULL,
    "total" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Order_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "Order_customerId_createdAt_idx" ON "Order"("customerId", "createdAt");

-- CreateIndex
CREATE INDEX "Order_status_createdAt_idx" ON "Order"("status", "createdAt");

-- CreateIndex
CREATE INDEX "Event_status_createdAt_idx" ON "Event"("status", "createdAt");

-- CreateIndex
CREATE INDEX "Event_type_createdAt_idx" ON "Event"("type", "createdAt");

-- CreateIndex
CREATE INDEX "IdempotencyRecord_status_updatedAt_idx" ON "IdempotencyRecord"("status", "updatedAt");

-- CreateIndex
CREATE INDEX "OutboxMessage_published_createdAt_idx" ON "OutboxMessage"("published", "createdAt");

-- CreateIndex
CREATE INDEX "OutboxMessage_eventType_createdAt_idx" ON "OutboxMessage"("eventType", "createdAt");
