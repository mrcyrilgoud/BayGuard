"""
Created 2025.04.02

Writes messages from the AirQuality System API to Kafka
"""

import requests
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
EMAIL = os.getenv("EMAIL")

BASE_URL = "https://aqs.epa.gov/data/api/"

KAFKA_BROKER = 'localhost:9092'  
KAFKA_TOPIC = 'air_quality_topic'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_air_quality_data():
    endpoint = "dailyData/byCounty"
    params = {
        "email": EMAIL,
        "key": API_KEY,
        "state": "06", # California
        "county": "075", # Santa Clara
        "param": "88101",  # PM2.5 - Local Conditions
        "bdate": "20220101",
        "edate": "20220101"
    }

    response = requests.get(BASE_URL + endpoint, params=params)

    if response.status_code == 200:
        return response.json()['Data']
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

def send_data_to_kafka(data):
    for record in data:
        producer.send(KAFKA_TOPIC, value=json.dumps(record))
    producer.flush()

if __name__ == "__main__":
    data = fetch_air_quality_data()

    if data:
        send_data_to_kafka(data)
        print(f"Sent {len(data)} records to Kafka.")
    else:
        print("No data to send.")
