import json
from confluent_kafka import Consumer, KafkaException, Producer

# Thresholds
TEMP_THRESHOLD = 03.0
COND_THRESHOLD = 1000.0
TURB_THRESHOLD = 10.0

# Kafka Configs
KAFKA_BROKER = "localhost:9092"
SOURCE_TOPIC = "water_quality"
ALERT_TOPIC = "water_quality_alerts"
GROUP_ID = "pollution-threshold-checker"

# Kafka Producer for alerts
def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER})

# Kafka Consumer for water quality messages
def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

# Check thresholds
def is_anomalous(record):
    try:
        temp = float(record.get("temperature", 0))
        cond = float(record.get("specific_conductance", 0))
        turb = float(record.get("turbidity", 0))

        return (
                temp > TEMP_THRESHOLD and
                cond > COND_THRESHOLD and
                turb > TURB_THRESHOLD
        )
    except Exception as e:
        print(f"Error parsing record: {e}")
        return False

# Main runner
def run():
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe([SOURCE_TOPIC])

    print(f"✅ Monitoring '{SOURCE_TOPIC}' for anomalies...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                record = json.loads(msg.value().decode("utf-8"))

                if is_anomalous(record):
                    print(f"🚨 Anomaly detected: {record['timestamp']} | Site: {record.get('site_name')}")
                    # Send alert message
                    producer.produce(ALERT_TOPIC, key=record.get("site_code", "alert"), value=json.dumps(record))
                    producer.flush()
                else:
                    print(f"✅ Normal reading: {record['timestamp']}")

            except json.JSONDecodeError:
                print("Invalid JSON received.")

    except KeyboardInterrupt:
        print("👋 Exiting consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
