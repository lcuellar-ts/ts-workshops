# Delete topic
podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --delete \
  --topic orders \
  --bootstrap-server localhost:9092

# List topics
podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

# Describe topics

podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server localhost:9092

# Modify an existing topic (add partitions)

podman exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --alter \
  --topic orders \
  --partitions 2 \
  --bootstrap-server localhost:9092