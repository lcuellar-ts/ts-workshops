const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "emails-group",
});

async function main() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "orders",
  });

  console.log("Email Service listening...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(
        "EMAIL",
        message.value.toString()
      );
    },
  });
}

main().catch(console.error);