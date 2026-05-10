require('dotenv').config();
const { Kafka } = require('kafkajs');
const { KafkaMessage, connectDb } = require('./db');

const kafka = new Kafka({
  clientId: 'tp9-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'tp9-group' });
const topic = process.env.KAFKA_TOPIC || 'test-topic';

const run = async () => {
  await connectDb();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  console.log(`Consommateur connecté, écoute du topic: ${topic}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value.toString();
      let payload;
      try {
        payload = JSON.parse(raw);
      } catch {
        payload = { raw };
      }

      const doc = await KafkaMessage.create({
        topic,
        partition,
        offset: message.offset,
        key: message.key ? message.key.toString() : null,
        payload,
      });

      console.log('Message consommé et sauvegardé:', {
        id: doc._id,
        topic,
        partition,
        offset: message.offset,
        payload,
      });
    },
  });
};

run().catch(console.error);
