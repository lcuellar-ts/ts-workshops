# Prerequisites
Before starting, install the Kafka command-line tools, create a working folder, and configure `client.properties` with the Aiven connection and authentication settings. Keep that file private because it contains sensitive connection details.

1. brew install kafka
2. Create ./kafka folder
3. Set up client.properties file

# 1. Topics
Create one topic per team or event stream. A topic is the named stream where producers publish messages and consumers read them. This example starts with one partition; the partition count can be increased later if needed.

````
kafka-topics \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --create \
  --topic {{TOPIC_NAME}} \
  --partitions 1
````

# 2. Consumer
In a terminal, start a consumer subscribed to your topic. The group name identifies this consumer group's progress: Kafka tracks offsets for each group and topic partition. Reuse the same group name when restarting if you want to continue from its committed offsets.

````
kafka-console-consumer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --consumer.config client.properties \
  --topic {{TOPIC_NAME}} \
  --group {{TOPIC_NAME}}-group
````

# 3. Producer

In a separate terminal, start a producer for the same topic. The producer publishes to a topic; it does not target a consumer group. Any consumer group subscribed to that topic can read the message.

````
kafka-console-producer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --producer.config client.properties \
  --topic {{TOPIC_NAME}}
````

Enter a message and press Enter to publish it; each line is a separate message. Messages can be plain text or JSON. For example:
{ "event": "notification.created", "notificationId": "ntf-001", "userId": 123, "type": "EMAIL", "message": "Hello from {{WHO_SENDS_MESSAGE}}", "timestamp": "2026-11-03T20:00:00Z" }

## Loop Message
This loop publishes a sample message every three seconds, which is useful for observing a running consumer. Stop it with Ctrl+C.

````
while true; do
  printf '%s\n' '{"event":"notification.created"}'
  sleep 3
done | kafka-console-producer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --producer.config client.properties \
  --topic {{TOPIC_NAME}}
````

# 4. Offset

Stop the consumer, publish several messages while it is offline, and then restart the consumer with the same group. It should resume from that group's committed offset and read messages that were published while it was stopped. Offsets are tracked separately for each partition, and a newly created group may start at the beginning or end depending on its offset-reset configuration.

# 5. Partitions
Partitions let a topic be split into independently ordered logs. Increasing the count can let multiple consumers in the same group read in parallel; Kafka assigns partitions among active group members. Existing messages are not moved when partitions are added.

````
kafka-topics \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --alter \
  --topic {{TOPIC_NAME}} \
  --partitions 2
````

For this exercise, assign partitions manually: use one consumer for partition 0 and another for partition 1. Manual assignment is different from subscribing with a consumer group; Kafka does not rebalance these manually assigned consumers.

Open separate terminals for the consumers. The command below manually reads partition 0; use the corresponding partition number for the other consumer.

````
kafka-console-consumer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --consumer.config client.properties \
  --topic {{TOPIC_NAME}} \
  --partition 0
````

To observe messages arriving in different partitions, publish a batch with the round-robin partitioner. This strategy distributes records across partitions; it is intended here as a demonstration and does not keep records for the same entity together.

````
kafka-console-producer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --producer.config client.properties \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
  --topic {{TOPIC_NAME}}
````

````
order-123:{"orderId":123,"event":"created"}
order-123:{"orderId":123,"event":"updated"}
order-456:{"orderId":456,"event":"created"}
order-123:{"orderId":123,"event":"updated"}
order-456:{"orderId":456,"event":"created"}
order-456:{"orderId":456,"event":"created"}
>
````

## Inject a large number of messages
This sends 1,000 sample messages to the topic. Use it to observe how records are distributed and consumed; stop any running producer with Ctrl+C.

````
for i in {1..1000}; do
  echo "message-$i"
done | kafka-console-producer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --producer.config client.properties \
  --topic {{TOPIC_NAME}}
````

# 6. Keys
Kafka keys let related records be routed to the same partition, preserving their order within that partition. With `parse.key` enabled, enter each record as `key:value`; the text before the separator is the key and the remainder is the message value. For example, records with the same order key go to the same partition. Avoid combining key-based routing with the round-robin strategy when you need that per-key ordering.

````
kafka-console-producer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --producer.config client.properties \
  --topic {{TOPIC_NAME}} \
  --property parse.key=true \
  --property key.separator=:
````

````
order-123:{"orderId":123,"event":"created"}
order-123:{"orderId":123,"event":"updated"}
order-456:{"orderId":456,"event":"created"}
order-123:{"orderId":123,"event":"updated"}
order-456:{"orderId":456,"event":"created"}
order-456:{"orderId":456,"event":"created"}
>
````

# 7. Replays

Replay means consuming records again by moving a consumer group's offsets backward. Stop all active consumers in the group before resetting its offsets. The first command resets the selected topic's partitions to their earliest retained records; the consumer can then read those records again with the same group.

````
kafka-consumer-groups \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --topic {{TOPIC_NAME}} \
  --group {{TOPIC_NAME}}-group \
  --reset-offsets \
  --to-earliest \
  --execute
````

````
kafka-console-consumer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --consumer.config client.properties \
  --topic {{TOPIC_NAME}} \
  --group {{TOPIC_NAME}}-group
````

You can also reset a single partition to a specific offset. Offsets are partition-local, not a single position shared by the whole topic. The example targets partition 1; replace the placeholder with the desired next offset. Run the reset while the group is inactive.

````
kafka-consumer-groups \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --group {{TOPIC_NAME}}-group \
  --topic {{TOPIC_NAME}}:1 \
  --reset-offsets --to-offset {{NEXT_TO_LAST_POSITION}} --execute
````

````
kafka-consumer-groups \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --describe \
  --group {{TOPIC_NAME}}-group
````

Use this command to inspect the group's committed offset and lag for each partition. The consumer's next command resumes from the group's stored offsets; it does not manually assign a partition.

````
kafka-console-consumer \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --consumer.config client.properties \
  --topic {{TOPIC_NAME}} \
  --group {{TOPIC_NAME}}-group
````

# Util Commands

## Check distribution of consumers on a Consumer Group
````
kafka-consumer-groups \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --describe \
  --group {{TOPIC_NAME}}-group \
  --members \
  --verbose
````

## Check Topic information & Partitions
````
kafka-consumer-groups \
  --bootstrap-server kafka-ts-kafka-techforge.l.aivencloud.com:25706 \
  --command-config client.properties \
  --describe \
  --group example-group
````