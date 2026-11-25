const { kafka } = require('./kafka');
const pool = require('./db');
require('dotenv').config();

const consumer = kafka.consumer({ groupId: 'csc337-order-inventory-group' });

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'tenant.orders', fromBeginning: true });

  console.log('====================================================');
  console.log('[Kafka Consumer] Listening on topic: tenant.orders');
  console.log('====================================================');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key ? message.key.toString() : 'no-key';
      const payload = JSON.parse(message.value.toString());

      console.log(`\n[Kafka Consumer] Message Received!`);
      console.log(` -> Topic: ${topic} | Partition: ${partition} | Offset: ${message.offset}`);
      console.log(` -> Tenant Key: ${key} | Order ID: ${payload.orderId}`);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Strict Tenant-Scoped Inventory Update
        for (const item of payload.items) {
          await client.query(
            `UPDATE products 
             SET stock = GREATEST(stock - $1, 0) 
             WHERE id = $2 AND tenant_id = $3`,
            [item.quantity, item.productId, payload.tenantId]
          );
        }

        // Order status ko PROCESSED mark karo (scoped to tenant_id)
        await client.query(
          `UPDATE orders SET status = 'PROCESSED' WHERE id = $1 AND tenant_id = $2`,
          [payload.orderId, payload.tenantId]
        );

        await client.query('COMMIT');
        console.log(` -> [DB Updated] Inventory decremented & Order #${payload.orderId} marked PROCESSED for ${payload.tenantId}`);
      } catch (err) {
        await client.query('ROLLBACK');
        console.error(' -> [Consumer Error]:', err.message);
      } finally {
        client.release();
      }
    },
  });
}

startConsumer().catch(console.error);