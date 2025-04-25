import requests
import json
import time
import random
from math import log
from datetime import datetime, timedelta
from confluent_kafka import Producer
from collections import defaultdict
from dotenv import load_dotenv
import os
import logging
import backoff

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("USGSProducer")

# Load environment variables
load_dotenv()
ENABLE_DP = os.getenv("ENABLE_DP", "true").lower() == "true"
SENSOR_IDS = os.getenv("SENSOR_IDS", "375603122254401,375607122264701,374811122235001,374938122251801").split(",")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "water_quality")

# USGS API URL
USGS_URL = f"http://waterservices.usgs.gov/nwis/iv/?format=json&sites={','.join(SENSOR_IDS)}&parameterCd=00010,00095,00300,00400,63680&siteStatus=ALL"

def create_producer():
    try:
        return Producer({"bootstrap.servers": KAFKA_BROKER})
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None

def laplace_noise(scale: float) -> float:
    u = random.uniform(-0.5, 0.5)
    return -scale * (1 if u < 0 else -1) * log(1 - 2 * abs(u))

def privatize_coordinate(value: float, epsilon: float = 1.0) -> float:
    scale = 0.0005 / epsilon
    return value + laplace_noise(scale)

def normalize_timestamp(ts):
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    rounded = dt - timedelta(minutes=dt.minute % 15, seconds=dt.second, microseconds=dt.microsecond)
    return rounded.isoformat()

def is_recent(timestamp: str, threshold_hours: int = 24) -> bool:
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        now = datetime.now(dt.tzinfo)
        return (now - dt).total_seconds() < threshold_hours * 3600
    except Exception:
        return False

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=5)
def fetch_data():
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()
    return response.json()

def parse_and_consolidate_data(data):
    consolidated = defaultdict(lambda: {
        "site_name": None,
        "site_code": None,
        "latitude": None,
        "longitude": None,
        "timestamp": None,
        "temperature": None,
        "specific_conductance": None,
        "turbidity": None,
        "source": "USGS",
        "retrieval_time": datetime.utcnow().isoformat()
    })

    for series in data.get("value", {}).get("timeSeries", []):
        site_info = series.get("sourceInfo", {})
        variable = series.get("variable", {})
        values = series.get("values", [])

        site_name = site_info.get("siteName")
        site_code = site_info.get("siteCode", [{}])[0].get("value")
        lat_raw = site_info.get("geoLocation", {}).get("geogLocation", {}).get("latitude")
        lon_raw = site_info.get("geoLocation", {}).get("geogLocation", {}).get("longitude")
        variable_name = variable.get("variableName", "").lower()

        if values and values[0].get("value"):
            latest = values[0]["value"][-1]
            raw_timestamp = latest.get("dateTime")

            if not is_recent(raw_timestamp):
                continue

            timestamp = normalize_timestamp(raw_timestamp)
            key = (site_code, timestamp)

            consolidated[key].update({
                "site_name": site_name,
                "site_code": site_code,
                "timestamp": timestamp,
                "latitude": privatize_coordinate(lat_raw) if ENABLE_DP else lat_raw,
                "longitude": privatize_coordinate(lon_raw) if ENABLE_DP else lon_raw
            })

            value = safe_float(latest.get("value"))
            if "temperature" in variable_name:
                consolidated[key]["temperature"] = value
            elif "specific conductance" in variable_name:
                consolidated[key]["specific_conductance"] = value
            elif "turbidity" in variable_name:
                consolidated[key]["turbidity"] = value

    return list(consolidated.values())

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"❌ Delivery failed: {err}")
    else:
        logger.info(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(producer, messages):
    if not producer:
        logger.warning("Kafka producer is not initialized.")
        return

    for msg in messages:
        try:
            producer.produce(KAFKA_TOPIC, key=msg.get("site_code", "unknown"), value=json.dumps(msg), callback=delivery_report)
        except BufferError:
            logger.warning("Kafka buffer full, waiting...")
            producer.poll(1)
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    producer.flush()

def main():
    producer = create_producer()
    try:
        while True:
            data = fetch_data()
            if data:
                msgs = parse_and_consolidate_data(data)
                if msgs:
                    send_to_kafka(producer, msgs)
                else:
                    logger.info("No messages to send.")
            else:
                logger.info("No data fetched.")

            logger.info("Sleeping for 15 minutes...")
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user.")
    finally:
        if producer:
            producer.flush()

if __name__ == "__main__":
    main()