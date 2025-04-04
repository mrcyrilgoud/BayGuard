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

API_KEY = os.getenv("AQS_API_KEY")
EMAIL = os.getenv("EMAIL")

if not API_KEY or not EMAIL:
    print("AQS_API_KEY or EMAIL is missing in the .env file")
    exit(1)

BASE_URL = "https://aqs.epa.gov/data/api/"

KAFKA_BROKER = 'localhost:9092'  
KAFKA_TOPIC = 'daily_air_quality'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def aqs_producer(endpoint, params, topic):

    base_url = "https://aqs.epa.gov/data/api/"

    response = requests.get(base_url + endpoint, params=params)

    if response.status_code == 200:
        print("Connection sucessful!")
    else:
        print(f"Error fetching data: {response.status_code}")
        return
    
    data = response.json()['Data']

    if data:
        # print(json.dumps(data, indent=4))
        for record in data:
            producer.send(topic, value=json.dumps(record))

        producer.flush()

        print(f"Sent {len(data)} records to {topic}.")
    else:
        print("No data to send.")

    return


if __name__ == "__main__":

    endpoint = "dailyData/byCounty"

    params = {
        "email": EMAIL,
        "key": API_KEY,
        "state": "06", # California
        "county": "085", # Santa Clara
        "param": "88101",  # PM2.5 - Local Conditions
        "bdate": "20200501",
        "edate": "20200930"
    }

    params2 = {
        "email": EMAIL,
        "key": API_KEY,
        "state": "06", # California
        "county": "075", # San Francisco
        "param": "88101",  # PM2.5 - Local Conditions
        "bdate": "20200501",
        "edate": "20200930"
    }

    params3 = {
        "email": EMAIL,
        "key": API_KEY,
        "state": "06", # California
        "county": "037", # Los Angeles
        "param": "88101",  # PM2.5 - Local Conditions
        "bdate": "20200101",
        "edate": "20201231"
    }
    
    aqs_producer(endpoint, params, KAFKA_TOPIC)
