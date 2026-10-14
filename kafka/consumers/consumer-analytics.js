const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "analytics-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "analytics-group",
});

async function main() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "orders",
  });

  console.log("Analytics Service listening...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(
        "ANALYTICS",
        message.value.toString()
      );
    },
  });
}

main().catch(console.error);