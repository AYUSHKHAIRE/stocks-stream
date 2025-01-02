#!/bin/bash

# Start Zookeeper server
echo "Starting Zookeeper server..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
sleep 5
# Start Kafka server
echo "Starting Kafka server..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
sleep 5
# Activate Python virtual environment
echo "Activating Python virtual environment..."
source kafkaenv/bin/activate
sleep 2
# Run Kafka consumer
echo "Running Kafka consumer..."
python3 consume.py &
sleep 2
# Run Kafka producer
echo "Running Kafka producer..."
python3 produce.py &
# confirm at last
sleep 5
echo "All processes are running."
echo "To stop, press Ctrl+C."
wait
