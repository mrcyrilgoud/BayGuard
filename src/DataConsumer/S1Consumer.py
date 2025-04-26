import json
import uuid
from confluent_kafka import Consumer, KafkaException, Producer
from collections import defaultdict
from redis import Redis
from dgim import Dgim  # Simon Dolle's DGIM class
from dotenv import load_dotenv
from sklearn.neighbors import NearestNeighbors
import numpy as np
import os

# Load environment variables
load_dotenv()

# Configuration
TEMP_THRESHOLD = float(os.getenv("TEMP_THRESHOLD", 25.0))
COND_THRESHOLD = float(os.getenv("COND_THRESHOLD", 1000.0))
TURB_THRESHOLD = float(os.getenv("TURB_THRESHOLD", 10.0))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "water_quality")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "water_quality_alerts")
GROUP_ID = os.getenv("GROUP_ID", "pollution-threshold-checker")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", 4))
ALERT_LOG_KEY = os.getenv("ALERT_LOG_KEY", "alert_logs")
LSH_NEIGHBORS = int(os.getenv("LSH_NEIGHBORS", 2))

# Initialize Redis and Kafka
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
site_alerted = set()
lsh_buckets = {}

def build_lsh_buckets():
    try:
        site_keys = redis_client.keys("*:latitude")
        site_codes = [key.split(":")[0] for key in site_keys]
        coords = []
        for code in site_codes:
            lat = redis_client.get(f"{code}:latitude")
            lon = redis_client.get(f"{code}:longitude")
            if lat and lon:
                coords.append([float(lat), float(lon)])
        if coords:
            model = NearestNeighbors(n_neighbors=LSH_NEIGHBORS + 1).fit(np.array(coords))
            distances, indices = model.kneighbors(np.array(coords))
            for i, site_code in enumerate(site_codes):
                lsh_buckets[site_code] = [site_codes[j] for j in indices[i] if site_codes[j] != site_code]
            print("LSH Buckets ready")
    except Exception as e:
        print(f"Error building LSH buckets: {e}")

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

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def get_dgim_violation_count(site, metric, threshold):
    key = f"{site}:{metric}"
    values = redis_client.lrange(key, 0, WINDOW_SIZE - 1)
    dgim = Dgim(N=WINDOW_SIZE)
    for val in values:
        try:
            dgim.update(float(val) > threshold)
        except:
            pass
    return dgim.get_count()

def validate_cross_site(site_code):
    if site_code not in lsh_buckets:
        build_lsh_buckets()
    peers = lsh_buckets.get(site_code, []) + [site_code]
    confirmed_sites = []
    for peer in peers:
        violations = 0
        for metric, threshold in zip(
                ["temperature", "specific_conductance", "turbidity"],
                [TEMP_THRESHOLD, COND_THRESHOLD, TURB_THRESHOLD]
        ):
            if get_dgim_violation_count(peer, metric, threshold) >= 2:
                violations += 1
        if violations >= 2:
            confirmed_sites.append(peer)
    return confirmed_sites if len(confirmed_sites) >= 2 else []

dgim_store = defaultdict(lambda: {
    'temperature': Dgim(N=WINDOW_SIZE),
    'specific_conductance': Dgim(N=WINDOW_SIZE),
    'turbidity': Dgim(N=WINDOW_SIZE)
})

def process_record(record):
    site = record.get("site_code")
    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat and lon:
        redis_client.set(f"{site}:latitude", lat)
        redis_client.set(f"{site}:longitude", lon)
    temp = safe_float(record.get("temperature"))
    cond = safe_float(record.get("specific_conductance"))
    turb = safe_float(record.get("turbidity"))

    if temp is not None:
        update_metric_cache(site, "temperature", temp)
        dgim_store[site]['temperature'].update(temp > TEMP_THRESHOLD)
    if cond is not None:
        update_metric_cache(site, "specific_conductance", cond)
        dgim_store[site]['specific_conductance'].update(cond > COND_THRESHOLD)
    if turb is not None:
        update_metric_cache(site, "turbidity", turb)
        dgim_store[site]['turbidity'].update(turb > TURB_THRESHOLD)

    violations = 0
    if temp is not None and dgim_store[site]['temperature'].get_count() >= 2:
        violations += 1
    if cond is not None and dgim_store[site]['specific_conductance'].get_count() >= 2:
        violations += 1
    if turb is not None and dgim_store[site]['turbidity'].get_count() >= 2:
        violations += 1

    return violations >= 2

def run():
    build_lsh_buckets()
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe([SOURCE_TOPIC])

    print(f"✅ Watching '{SOURCE_TOPIC}' for threshold violations...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                record = json.loads(msg.value().decode("utf-8"))
                site_code = record.get("site_code")

                if process_record(record):
                    confirmed_group = validate_cross_site(site_code)
                    alert_id = str(uuid.uuid4())
                    if confirmed_group and not site_alerted.intersection(set(confirmed_group)):
                        alert = {
                            "alert_id": alert_id,
                            "type": "CrossSiteValidatedAlert",
                            "lsh_buckets": lsh_buckets,
                            "group_sites": confirmed_group,
                            "source": "RBConsumer.py",
                            "timestamp": record.get("timestamp")
                        }
                        producer.produce(ALERT_TOPIC, key=site_code, value=json.dumps(alert))
                        producer.flush()
                        redis_client.lpush(ALERT_LOG_KEY, json.dumps(alert))
                        redis_client.ltrim(ALERT_LOG_KEY, 0, 49)
                        site_alerted.update(confirmed_group)
                        print(f"🚨 CrossSiteValidatedAlert sent for group: {confirmed_group}")
                    elif not confirmed_group:
                        alert = {
                            "alert_id": alert_id,
                            "type": "UnifiedThresholdAlert",
                            "metrics": {
                                "temperature": get_metric_average(site_code, "temperature"),
                                "specific_conductance": get_metric_average(site_code, "specific_conductance"),
                                "turbidity": get_metric_average(site_code, "turbidity")
                            },
                            "source": "RBConsumer.py",
                            "sensor": site_code,
                            "site_name": record.get("site_name"),
                            "latitude": record.get("latitude"),
                            "longitude": record.get("longitude"),
                            "timestamp": record.get("timestamp")
                        }
                        producer.produce(ALERT_TOPIC, key=alert["sensor"], value=json.dumps(alert))
                        producer.flush()
                        redis_client.lpush(ALERT_LOG_KEY, json.dumps(alert))
                        redis_client.ltrim(ALERT_LOG_KEY, 0, 49)
                        print(f"🚨 UnifiedThresholdAlert sent for {site_code} at {record['timestamp']}")

            except json.JSONDecodeError:
                print("Invalid JSON received.")

    except KeyboardInterrupt:
        print("👋 Exiting consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
