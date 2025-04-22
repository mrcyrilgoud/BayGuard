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

# Load environment variables
load_dotenv()
ENABLE_DP = os.getenv("ENABLE_DP", "true").lower() == "true"

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "water_quality"

# Sensor IDs
SENSOR_IDS = [
    "375603122254401",  # Richmond Bridge Pier 47
    "375607122264701",  # Richmond Bridge Sensor 2
    "374811122235001",  # Alcatraz
    "374938122251801",  # SF Pier 17
]

# USGS API URL
USGS_URL = f"http://waterservices.usgs.gov/nwis/iv/?format=json&sites={','.join(SENSOR_IDS)}&parameterCd=00010,00095,00300,00400,63680&siteStatus=ALL"

# Create Kafka producer
def create_producer():
    try:
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

# Add Laplace noise for differential privacy
def laplace_noise(scale: float) -> float:
    u = random.uniform(-0.5, 0.5)
    return -scale * (1 if u < 0 else -1) * log(1 - 2 * abs(u))

def privatize_coordinate(value: float, epsilon: float = 1.0) -> float:
    scale = 0.0005 / epsilon
    return value + laplace_noise(scale)

# Normalize timestamp to nearest 15 minutes
def normalize_timestamp(ts):
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    rounded = dt - timedelta(minutes=dt.minute % 15, seconds=dt.second, microseconds=dt.microsecond)
    return rounded.isoformat()

# Check if a timestamp is recent (within threshold_hours)
def is_recent(timestamp: str, threshold_hours: int = 24) -> bool:
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        now = datetime.now(dt.tzinfo)
        return (now - dt).total_seconds() < threshold_hours * 3600
    except Exception:
        return False

# Fetch data
def fetch_data():
    try:
        response = requests.get(USGS_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Parse and consolidate data
def parse_and_consolidate_data(data):
    consolidated = defaultdict(lambda: {
        "site_name": None,
        "site_code": None,
        "latitude": None,
        "longitude": None,
        "timestamp": None,
        "temperature": None,
        "specific_conductance": None,
        "turbidity": None
    })

    try:
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
                    continue  # Skip stale data

                timestamp = normalize_timestamp(raw_timestamp)
                key = (site_code, timestamp)

                consolidated[key]["site_name"] = site_name
                consolidated[key]["site_code"] = site_code
                consolidated[key]["timestamp"] = timestamp

                if ENABLE_DP:
                    consolidated[key]["latitude"] = privatize_coordinate(lat_raw)
                    consolidated[key]["longitude"] = privatize_coordinate(lon_raw)
                else:
                    consolidated[key]["latitude"] = lat_raw
                    consolidated[key]["longitude"] = lon_raw

                if "temperature" in variable_name:
                    consolidated[key]["temperature"] = latest.get("value")
                elif "specific conductance" in variable_name:
                    consolidated[key]["specific_conductance"] = latest.get("value")
                elif "turbidity" in variable_name:
                    consolidated[key]["turbidity"] = latest.get("value")

    except Exception as e:
        print(f"Error parsing and consolidating JSON data: {e}")

    return list(consolidated.values())

# Delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"\u274c Delivery failed: {err}")
    else:
        print(f"\u2705 Message delivered to {msg.topic()} [{msg.partition()}]")

# Send to Kafka
def send_to_kafka(producer, messages):
    if not producer:
        print("Kafka producer is not initialized.")
        return

    for msg in messages:
        try:
            producer.produce(KAFKA_TOPIC, key=msg.get("site_code", "unknown"), value=json.dumps(msg), callback=delivery_report)
        except BufferError:
            print("Kafka buffer full, waiting...")
            producer.poll(1)
        except Exception as e:
            print(f"Error producing message: {e}")

    producer.flush()

# Main loop every 15 minutes
if __name__ == "__main__":
    producer = create_producer()

    while True:
        data = fetch_data()
        if data:
            msgs = parse_and_consolidate_data(data)
            if msgs:
                send_to_kafka(producer, msgs)
            else:
                print("No messages to send.")
        else:
            print("No data fetched.")

        print("Sleeping for 15 minutes...")
        time.sleep(900)

