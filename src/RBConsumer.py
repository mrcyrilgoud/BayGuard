import json
from confluent_kafka import Consumer, KafkaException, Producer
from collections import defaultdict
from redis import Redis
from dgim import Dgim  # Simon Dolle's DGIM class
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration
TEMP_THRESHOLD = float(os.getenv("TEMP_THRESHOLD", 3.0))
COND_THRESHOLD = float(os.getenv("COND_THRESHOLD", 1000.0))
TURB_THRESHOLD = float(os.getenv("TURB_THRESHOLD", 10.0))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "water_quality")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "water_quality_alerts")
GROUP_ID = os.getenv("GROUP_ID", "pollution-threshold-checker")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", 4))  # last 1 hour if sampling every 15 min

# Initialize Redis and Kafka
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

def update_metric_cache(site_code, metric, value):
    key = f"{site_code}:{metric}"
    redis_client.lpush(key, value)
    redis_client.ltrim(key, 0, WINDOW_SIZE - 1)

def get_metric_average(site_code, metric):
    key = f"{site_code}:{metric}"
    values = redis_client.lrange(key, 0, -1)
    float_vals = [float(v) for v in values if v not in (None, "")]
    return sum(float_vals) / len(float_vals) if float_vals else None

dgim_store = defaultdict(lambda: {
    'temperature': Dgim(N=WINDOW_SIZE),
    'specific_conductance': Dgim(N=WINDOW_SIZE),
    'turbidity': Dgim(N=WINDOW_SIZE)
})

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def process_record(record):
    site = record.get("site_code")
    temp = safe_float(record.get("temperature"))
    cond = safe_float(record.get("specific_conductance"))
    turb = safe_float(record.get("turbidity"))

    # Update Redis cache and DGIM only if value is present
    if temp is not None:
        update_metric_cache(site, "temperature", temp)
        dgim_store[site]['temperature'].update(temp > TEMP_THRESHOLD)
    if cond is not None:
        update_metric_cache(site, "specific_conductance", cond)
        dgim_store[site]['specific_conductance'].update(cond > COND_THRESHOLD)
    if turb is not None:
        update_metric_cache(site, "turbidity", turb)
        dgim_store[site]['turbidity'].update(turb > TURB_THRESHOLD)

    # Estimate DGIM violations only on non-null metrics
    violations = 0
    if temp is not None and dgim_store[site]['temperature'].get_count() >= 2:
        violations += 1
    if cond is not None and dgim_store[site]['specific_conductance'].get_count() >= 2:
        violations += 1
    if turb is not None and dgim_store[site]['turbidity'].get_count() >= 2:
        violations += 1

    return violations >= 2  # Trigger alert if 2+ metrics are frequently violated

def run():
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe([SOURCE_TOPIC])

    print(f"\u2705 Watching '{SOURCE_TOPIC}' for threshold violations...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                record = json.loads(msg.value().decode("utf-8"))

                if process_record(record):
                    alert = {
                        "type": "UnifiedThresholdAlert",
                        "metrics": {
                            "temperature": get_metric_average(record['site_code'], "temperature"),
                            "specific_conductance": get_metric_average(record['site_code'], "specific_conductance"),
                            "turbidity": get_metric_average(record['site_code'], "turbidity")
                        },
                        "source": "RBConsumer.py",
                        "sensor": record.get("site_code"),
                        "site_name": record.get("site_name"),
                        "latitude": record.get("latitude"),
                        "longitude": record.get("longitude"),
                        "timestamp": record.get("timestamp")
                    }
                    producer.produce(ALERT_TOPIC, key=alert["sensor"], value=json.dumps(alert))
                    producer.flush()
                    print(f"\U0001F6A8 Alert sent for {record['site_code']} at {record['timestamp']}")

            except json.JSONDecodeError:
                print("Invalid JSON received.")

    except KeyboardInterrupt:
        print("\U0001F44B Exiting consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()






