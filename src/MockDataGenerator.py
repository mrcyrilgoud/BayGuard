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

# Site metadata with real coordinates
SITE_METADATA = {
    "375603122254401": {
        "site_name": "SAN FRANCISCO BAY A RICHMOND-SAN RAFAEL BR PIER 47",
        "latitude": 37.93426389,
        "longitude": -122.428861,
    },
    "375607122264701": {
        "site_name": "RICHMOND BRIDGE SENSOR 2",
        "latitude": 37.9286,
        "longitude": -122.4572,
    },
    "374811122235001": {
        "site_name": "SAN FRANCISCO BAY A PIER 17 A SAN FRANCISCO CA",
        "latitude": 37.80305,
        "longitude": -122.3973083,
    },
    "374938122251801": {
        "site_name": "ALCATRAZ ISLAND SENSOR",
        "latitude": 37.8267,
        "longitude": -122.4230,
    },
}

USGS_URL = f"http://waterservices.usgs.gov/nwis/iv/?format=json&sites={','.join(SENSOR_IDS)}&parameterCd=00010,00095,00300,00400,63680&siteStatus=ALL"

# Create Kafka producer
def create_producer():
    try:
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

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

# Fetch real data from USGS for fallback sites
def fetch_data():
    try:
        response = requests.get(USGS_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Parse and consolidate USGS data with stale data prevention
def parse_and_consolidate_data(data, site_filter):
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

            site_code = site_info.get("siteCode", [{}])[0].get("value")
            if site_code not in site_filter:
                continue

            site_name = site_info.get("siteName")
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
                    consolidated[key]["latitude"] = lat_raw + random.uniform(-0.0005, 0.0005)
                    consolidated[key]["longitude"] = lon_raw + random.uniform(-0.0005, 0.0005)
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
        print(f"Error parsing USGS data: {e}")

    return list(consolidated.values())

# Generate mock pollution data for given site IDs
def generate_pollution_data(site_ids):
    now = datetime.utcnow().isoformat()
    mock_data = []

    trigger_temp = os.getenv("TRIGGER_TEMP", "true").lower() == "true"
    trigger_cond = os.getenv("TRIGGER_COND", "true").lower() == "true"
    trigger_turb = os.getenv("TRIGGER_TURB", "true").lower() == "true"

    for site_id in site_ids:
        site = SITE_METADATA[site_id]
        mock_data.append({
            "site_name": site["site_name"],
            "site_code": site_id,
            "latitude": site["latitude"],
            "longitude": site["longitude"],
            "timestamp": normalize_timestamp(now),
            "temperature": 50.0 if trigger_temp else 10.0,
            "specific_conductance": 5000.0 if trigger_cond else 800.0,
            "turbidity": 35.0 if trigger_turb else 4.0,
        })

    return mock_data

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

# Main loop
def main():
    producer = create_producer()
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
    polluted_sites = os.getenv("POLLUTED_SITES", "")
    polluted_ids = [s.strip() for s in polluted_sites.split(",") if s.strip()]

    while True:
        msgs = []
        if test_mode:
            print(f"\U0001f9ea TEST_MODE active: Simulating for sites {polluted_ids}")
            if polluted_ids:
                msgs.extend(generate_pollution_data(polluted_ids))
            real_ids = [sid for sid in SENSOR_IDS if sid not in polluted_ids]
            raw = fetch_data()
            if raw:
                msgs.extend(parse_and_consolidate_data(raw, real_ids))
        else:
            print("Live mode not implemented in this script.")

        if msgs:
            send_to_kafka(producer, msgs)
        else:
            print("No messages to send.")

        print("Sleeping for 15 minutes...")
        time.sleep(30)

if __name__ == "__main__":
    main()

