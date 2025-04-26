from confluent_kafka import Consumer, KafkaException
import json
import openai
import os
from dotenv import load_dotenv
from redis import Redis

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "water_quality_alerts"
GROUP_ID = "stage2-verifier-unified"

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

# Redis for alert logs
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
ALERT_LOG_KEY = "alert_logs"
VERIFIED_ALERT_KEY = "verified_alerts"

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

def backfill_past_alerts():
    print("🔎 Running backfill of past alerts...")
    try:
        past_alerts = redis_client.lrange(ALERT_LOG_KEY, 0, 49)
        verified_alerts = redis_client.lrange(VERIFIED_ALERT_KEY, 0, 49)
        verified_timestamps = {json.loads(a).get("timestamp") for a in verified_alerts if a}

        for raw_alert in past_alerts:
            alert = json.loads(raw_alert)
            if alert.get("timestamp") not in verified_timestamps and alert.get("type") in ["UnifiedThresholdAlert", "CrossSiteValidatedAlert"]:
                process_alert_with_llm(alert)
    except Exception as e:
        print(f"❌ Backfill error: {e}")

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
                if alert.get("type") in ["UnifiedThresholdAlert", "CrossSiteValidatedAlert"]:
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
                {"role": "system", "content": "You are an environmental compliance analyst. Evaluate water quality alerts and determine if they likely indicate industrial pollution."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )
        result = response.choices[0].message.content
        print("\n🧠 AI Reasoning Result:\n", result)

        # Attach LLM analysis
        alert['llm_verification'] = result

        # Ensure alert_id
        alert_id = alert.get('alert_id')
        if not alert_id:
            import uuid
            alert_id = str(uuid.uuid4())
            alert['alert_id'] = alert_id

        # Save using HSET (not LPUSH!)
        redis_client.hset(VERIFIED_ALERT_KEY, alert_id, json.dumps(alert))

    except Exception as e:
        print(f"❌ LLM reasoning failed: {e}")

def build_prompt(alert):
    alert_type = alert.get("type")

    if alert_type == "CrossSiteValidatedAlert":
        sites = alert.get("group_sites", [])
        site_info = "\n".join([f"- {sid}" for sid in sites])
        return f"""
Cross-Site Validated Alert

Group of sensors that simultaneously exceeded thresholds:
{site_info}

LSH Buckets:
{json.dumps(alert.get('lsh_buckets', {}), indent=2)}

Timestamp: {alert.get('timestamp')}
Source: {alert.get('source')}

Evaluate whether this cross-site alert suggests coordinated pollution activity. Provide a confidence score (0.0–1.0) and rationale.
"""

    elif alert_type == "UnifiedThresholdAlert":
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
    backfill_past_alerts()  #  Backfill old alerts first
    consume_messages()      #  Then listen for new alerts

