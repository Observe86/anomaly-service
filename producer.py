from confluent_kafka import Producer
import os
import json

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
PRODUCE_TOPIC = os.getenv("KAFKA_PRODUCE_TOPICS", "anomalies")

producer_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

def produce_anomaly(event: dict):
    """Send anomaly event to Kafka"""
    producer.produce(PRODUCE_TOPIC, value=json.dumps(event).encode("utf-8"))
    producer.flush()
