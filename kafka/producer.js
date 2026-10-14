const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "orders-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function main() {
  await producer.connect();

  let count = 1;

  console.log("Producing messages...");

  setInterval(async () => {
    const event = {
      id: count,
      timestamp: new Date().toISOString(),
      type: "OrderCreated",
    };

    await producer.send({
      topic: "orders",
      messages: [
        {
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("Sent:", event);

    count++;
  }, 2000);
}

main().catch(console.error);