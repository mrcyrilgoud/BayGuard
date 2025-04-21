from confluent_kafka import Consumer, KafkaException
import json
import openai
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "water_quality_alerts"
GROUP_ID = "stage2-verifier-unified"

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })


def consume_messages():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    print(f"\n🔁 Listening to '{KAFKA_TOPIC}' for alerts...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                if alert.get("type") == "UnifiedThresholdAlert":
                    process_alert_with_llm(alert)
                else:
                    print("ℹ️ Ignored non-threshold alert.")
            except Exception as e:
                print(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        print("🛑 Shutting down consumer.")
    finally:
        consumer.close()


def process_alert_with_llm(alert):
    prompt = build_prompt(alert)

    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an environmental compliance analyst. Evaluate unified water quality alerts and determine if they likely indicate industrial pollution."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )
        result = response.choices[0].message.content
        print("\n🧠 AI Reasoning Result:\n", result)

    except Exception as e:
        print(f"❌ LLM reasoning failed: {e}")


def build_prompt(alert):
    site = alert.get("site_name")
    time = alert.get("timestamp")
    source = alert.get("source")
    sensor = alert.get("sensor")
    coords = f"({alert.get('latitude')}, {alert.get('longitude')})"
    metrics = alert.get("metrics", {})

    metrics_text = "\n".join([f"- {k}: {v}" for k, v in metrics.items()])

    return f"""
Unified Threshold Alert received from {source}:

Site: {site}
Sensor: {sensor}
Coordinates: {coords}
Timestamp: {time}

Metrics that exceeded thresholds:
{metrics_text}

Evaluate whether this alert indicates a likely industrial pollution event. Provide a confidence score (0.0–1.0) and rationale.
"""


if __name__ == "__main__":
    consume_messages()

