from confluent_kafka import Consumer, KafkaException
import json
import sys

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "water_quality_alerts"
GROUP_ID = "stage2-verification-group"

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

def process_anomalous_message(message_data):
    print("🚨 Anomaly Detected – Triggering Stage 2 Verification")
    # Placeholder for stage 2 logic
    # You can call EPA/ECHO/Air API and AI verification functions here
    print(json.dumps(message_data, indent=2))

def consume_messages():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    try:
        print(f"🔁 Listening on topic: {KAFKA_TOPIC} ...")
        while True:
            msg = consumer.poll(1.0)  # Wait for message or timeout
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                message_data = json.loads(msg.value().decode("utf-8"))
                if message_data.get("anomaly") is True:
                    process_anomalous_message(message_data)
                else:
                    print("ℹ️ Received non-anomalous message, ignoring.")

            except json.JSONDecodeError:
                print("❌ Failed to decode message JSON.")

    except KeyboardInterrupt:
        print("🛑 Shutting down consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()