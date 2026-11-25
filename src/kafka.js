const { Kafka } = require('kafkajs');
require('dotenv').config();

const brokers = (process.env.KAFKA_BROKERS || '').split(',').map((b) => b.trim()).filter(Boolean);

let producer = null;
let isProducerConnected = false;

function createKafkaClient() {
  if (!brokers.length || brokers[0] === 'localhost:9092') {
    return null;
  }
  const kafkaConfig = {
    clientId: 'csc337-multi-tenant-saas',
    brokers,
  };
  if (process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
    kafkaConfig.ssl = true;
    kafkaConfig.sasl = {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256',
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    };
  }
  return new Kafka(kafkaConfig);
}

const kafka = createKafkaClient();

async function publishOrderEvent(tenantId, orderPayload) {
  const eventData = {
    event: 'OrderCreated',
    tenantId,
    timestamp: new Date().toISOString(),
    ...orderPayload,
  };

  if (!kafka) {
    console.log(`[Kafka Simulation] Broker not configured yet -> Queued event for ${tenantId}, Order #${orderPayload.orderId}`);
    return [{ partition: 0, baseOffset: String(Date.now()) }];
  }

  if (!producer) {
    producer = kafka.producer();
  }
  if (!isProducerConnected) {
    await producer.connect();
    isProducerConnected = true;
  }

  const record = await producer.send({
    topic: 'tenant.orders',
    messages: [
      {
        key: String(tenantId),
        value: JSON.stringify(eventData),
      },
    ],
  });

  console.log(`[Kafka Producer] Topic: tenant.orders | Key: ${tenantId} | Partition: ${record[0].partition} | Offset: ${record[0].baseOffset}`);
  return record;
}

module.exports = { kafka, publishOrderEvent };