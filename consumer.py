from confluent_kafka import Consumer
from service.detection import detect_anomaly
from producer import produce_anomaly
import os
import json

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
CONSUME_TOPICS = os.getenv("KAFKA_CONSUME_TOPICS", "metrics,logs,traces").split(",")

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "anomaly-service",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe(CONSUME_TOPICS)

print(f"Anomaly Service consuming from {CONSUME_TOPICS}, producing to {os.getenv('KAFKA_PRODUCE_TOPICS')}")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("⚠️ Consumer error:", msg.error())
        continue

    raw_data = msg.value().decode("utf-8")
    try:
        data = json.loads(raw_data)
    except Exception:
        print("Failed to parse message:", raw_data)
        continue

    is_anomaly, score = detect_anomaly(data)

    if is_anomaly:
        anomaly_event = {
            "source_topic": msg.topic(),
            "data": data,
            "score": score,
        }
        produce_anomaly(anomaly_event)
        print(f"Anomaly detected: {anomaly_event}")
