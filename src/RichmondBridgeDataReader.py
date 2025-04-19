import requests
import json
import time
from confluent_kafka import Producer
from collections import defaultdict

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "water_quality"

# Create Kafka producer
def create_producer():
    try:
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

# API Endpoint
USGS_URL = "http://waterservices.usgs.gov/nwis/iv/?format=json&sites=375603122254401&parameterCd=00010,00095,00300,00400,63680&siteStatus=ALL"

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
            latitude = site_info.get("geoLocation", {}).get("geogLocation", {}).get("latitude")
            longitude = site_info.get("geoLocation", {}).get("geogLocation", {}).get("longitude")
            variable_name = variable.get("variableName", "").lower()
            unit = variable.get("unit", {}).get("unitCode")

            if values and values[0].get("value"):
                latest = values[0]["value"][-1]  # Get the most recent value
                timestamp = latest.get("dateTime")
                key = (site_code, timestamp)

                consolidated[key]["site_name"] = site_name
                consolidated[key]["site_code"] = site_code
                consolidated[key]["latitude"] = latitude
                consolidated[key]["longitude"] = longitude
                consolidated[key]["timestamp"] = timestamp

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
        time.sleep(900)  # Sleep for 15 minutes (900 seconds)
