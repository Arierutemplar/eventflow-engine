import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export async function clearDatabase(): Promise<void> {
  await prisma.$transaction([
    prisma.outboxMessage.deleteMany(),
    prisma.idempotencyRecord.deleteMany(),
    prisma.event.deleteMany(),
  ]);
}

beforeEach(async () => {
  await clearDatabase();
});

afterAll(async () => {
  await prisma.$disconnect();
});
