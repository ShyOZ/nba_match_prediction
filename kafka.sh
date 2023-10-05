#!/bin/bash

# Change directory to Kafka installation folder
cd /usr/local/kafka/kafka_2.13-3.2.1

# Start ZooKeeper server
./bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait until ZooKeeper is up
echo "Waiting for ZooKeeper to start..."
while ! nc -z localhost 2181; do
  sleep 1
done
echo "ZooKeeper is up and running!"

# Start Kafka server in a new terminal
gnome-terminal -- ./bin/kafka-server-start.sh config/server.properties &

sleep 5
# Create a new Kafka topic in a new terminal
gnome-terminal -- bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic nba &

# List existing Kafka topics
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092
