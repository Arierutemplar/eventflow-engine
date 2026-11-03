import { prisma } from '@eventflow/database';

async function main() {
  console.log(' [*] Insertando evento de prueba en la tabla Outbox...');

  // Simulamos un evento de negocio (ej. Orden creada)
  const event = await prisma.outboxMessage.create({
    data: {
      eventType: 'order.created',
      payload: {
        orderId: 'ord_' + Math.random().toString(36).substring(2, 9),
        item: 'QJMotor SRK 600 RC Exhaust Kit',
        amount: 450.0,
        currency: 'USD',
        timestamp: new Date().toISOString(),
      },
      published: false,
    },
  });

  console.log(` [x] Evento Outbox guardado exitosamente con ID: ${event.id}`);
}

main()
  .catch((e) => {
    console.error(' [!] Error ejecutando el seed:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });