# Kafka Pub/Sub Lab with Podman

This lab assumes **zero Kafka experience** and **zero Podman experience**.

By the end of the lab you will understand:

- What a Topic is
- What a Partition is
- What an Offset is
- What a Consumer Group is
- How Kafka Pub/Sub works
- Why multiple Consumer Groups can read the same messages
- How Kafka differs from Redis Pub/Sub

---

# Mental Model

Imagine an Order Service.

Instead of directly calling Email Service and Analytics Service:

```text
Order Service
   |
   +--> Email Service
   |
   +--> Analytics Service
```

it publishes an event:

```text
Order Service
      |
      v

+-------------------+
| Kafka Topic       |
| orders            |
+-------------------+
      |
      |
      +--> Email Service
      |
      +--> Analytics Service
```

The producer does not know who consumes the events.

---

# Kafka Concepts

## Topic

A Topic is a category of events.

Examples:

```text
orders
payments
users
notifications
```

For this lab:

```text
Topic = orders
```

---

## Partition

A Topic is divided into one or more partitions.

Example:

```text
orders

├── Partition 0
├── Partition 1
└── Partition 2
```

Partitions enable parallel processing.

Think of them as lanes on a highway.

Without partitions:

```text
🚗
🚗
🚗
🚗
```

With partitions:

```text
🚗 🚗 🚗
🚗 🚗 🚗
🚗 🚗 🚗
```

---

## Offset

Every message inside a partition gets a position.

Example:

```text
Partition 0

Offset 0 -> OrderCreated1001

Offset 1 -> OrderCreated1002

Offset 2 -> OrderCreated1003
```

Offsets make replay possible.

---

## Consumer Group

A Consumer Group represents an application.

Example:

```text
emails-group

analytics-group
```

Each group keeps track of its own offsets.

This means multiple groups can read the same messages independently.

---

# Important Rule

## Different Consumer Groups

```text
Topic orders

      |
      +--> emails-group

      |
      +--> analytics-group
```

Result:

```text
emails-group receives:
1001
1002
1003

analytics-group receives:
1001
1002
1003
```

Both groups get everything.

This is Pub/Sub.

---

## Same Consumer Group

```text
analytics-group

Consumer A
Consumer B
Consumer C
```

Result:

```text
Message 1 -> Consumer A

Message 2 -> Consumer B

Message 3 -> Consumer C
```

Kafka distributes work.

This is called competing consumers.

---

# Prerequisites

Verify Podman installation:

```bash
podman --version
```

Expected:

```text
podman version x.x.x
```

Verify Compose support:

```bash
podman compose version
```

---

# Create Project Folder

```bash
mkdir kafka-lab

cd kafka-lab
```

---

# Create compose.yaml

Create a file named:

```text
compose.yaml
```

Paste:

```yaml
services:
  kafka:
    image: docker.io/apache/kafka:4.0.0
    container_name: kafka

    ports:
      - "9092:9092"

    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093

      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

---

# Start Kafka

```bash
podman compose up -d
```

Verify:

```bash
podman ps
```

Expected:

```text
CONTAINER ID
...
kafka
```

---

# Check Logs

```bash
podman logs kafka
```

Look for messages similar to:

```text
Kafka Server started
```

or

```text
Transitioning from RECOVERY to RUNNING
```

---
<!-- DEMO KAFKA -->

# Create a Topic

Create the topic named:

```text
orders
```

Run:

```bash
podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092
```

---

# Verify Topic Exists

```bash
podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
orders
```

---

# Inspect Topic

```bash
podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server localhost:9092
```

Expected:

```text
Topic: orders
PartitionCount: 1
ReplicationFactor: 1
```

---
<!-- KAFKA PUB/SUB -->

# Start Consumer Group #1

Open Terminal #2.

Run:

```bash
podman exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --group emails
```

Leave it running.

---

# Start Consumer Group #2

Open Terminal #3.

Run:

```bash
podman exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --group analytics
```

Leave it running.

---

# Start Producer

Open Terminal #4.

Run:

```bash
podman exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server localhost:9092
```

Kafka is now waiting for messages.

---

# Publish Events

Type:

```text
OrderCreated:1001
```

Press ENTER.

Then:

```text
OrderCreated:1002
```

Press ENTER.

Then:

```text
OrderCreated:1003
```

Press ENTER.

---

# Observe Results

## Terminal emails

```text
OrderCreated:1001
OrderCreated:1002
OrderCreated:1003
```

## Terminal analytics

```text
OrderCreated:1001
OrderCreated:1002
OrderCreated:1003
```

Success!

Both consumer groups received every event.

This is Kafka Pub/Sub.

---
<!-- LIST "SUBSCRIBERS" -->

# List Consumer Groups

Open another terminal.

Run:

```bash
podman exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected:

```text
analytics
emails
```

---

# Inspect Offsets

Run:

```bash
podman exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group emails \
  --describe
```

Expected output similar to:

```text
GROUP        TOPIC    PARTITION    CURRENT-OFFSET

emails       orders   0            3
```

Meaning:

```text
Offset 0 -> OrderCreated:1001
Offset 1 -> OrderCreated:1002
Offset 2 -> OrderCreated:1003
```

The group has consumed all messages.

---
<!-- REPLAY -->

# Demonstrating Replay

Stop Analytics Consumer:

```text
CTRL + C
```

Now publish:

```text
OrderCreated:1004
```

from the Producer terminal.

Restart Analytics Consumer:

```bash
podman exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --group analytics
```

Kafka remembers the group's offset.

This behavior is possible because Kafka stores messages durably.

In Japanese:

```text
永続化（えいぞくか）
Persistence
```

---

# What You Learned

## Topic

A category of events.

Example:

```text
orders
```

---

## Partition

A subdivision of a topic.

Example:

```text
orders

Partition 0
Partition 1
Partition 2
```

Used for parallelism.

---

## Offset

Position of a message inside a partition.

Example:

```text
Offset 0
Offset 1
Offset 2
```

Used for replay.

---

## Consumer Group

A logical application consuming messages.

Example:

```text
emails-group

analytics-group
```

Each group tracks its own offsets.

---

# Can Two Consumer Groups Read the Same Offset?

Yes.

Example:

```text
Topic orders

Partition 0

Offset 0 -> Order1001

Offset 1 -> Order1002
```

Both groups can read:

```text
emails-group -> Offset 0

analytics-group -> Offset 0
```

independently.

Kafka stores offsets per consumer group, not globally.

---

# Cleanup

Stop Kafka:

```bash
podman compose down
```

Remove everything including volumes:

```bash
podman compose down -v
```

WARNING:

```text
-v deletes local Kafka data.
```

---
<!-- DEMO 2: PARTITIONS -->

# Next Exercise

Create:

```text
Topic orders
```

with:

```text
3 partitions
```

Then create:

```text
analytics-group

Consumer A
Consumer B
Consumer C
```

Observe how Kafka automatically distributes partitions among consumers.

This is where Kafka's horizontal scaling really starts to make sense.

# Create a topic with 4 partitions

podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Listen into 1 consumer group

## Run Terminal 1: Consumer group

podman exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group analytics-group

## Run Terminal 2: Consumer group

podman exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group analytics-group

## Run Terminal 3: Producer (Sender of events)

podman exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders

### With keys

podman exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:

## Send the info in the producer

user1:order-1
user2:order-2
user3:order-3
user4:order-4
user5:order-5
user6:order-6
user7:order-7
user8:order-8
user9:order-9
user10:order-10
user11:order-11
user12:order-12
user13:order-13
user14:order-14
user15:order-15
user16:order-16
user17:order-17
user18:order-18
user19:order-19
user20:order-20

# Expected outcome

**CORRECT: Something similar to**
Consumer A
order-1
order-3
order-5
order-7

Consumer B
order-2
order-4
order-6
order-8

**NOT:**
Consumer A
order-1
order-2
order-3
order-4

Consumer B
order-1
order-2
order-3
order-4

# View the partitions associated to each consumer

podman exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --describe

# Closing 1 consumer distributes the partitions among the active consumers

podman exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --describe