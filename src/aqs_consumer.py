"""
Created 2025.04.03

Reads messages from Kafka
"""

from confluent_kafka import Consumer, KafkaException, KafkaError
import json

KAFKA_TOPIC = "daily_air_quality"

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'aqs_group',  
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([KAFKA_TOPIC]) 

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8') 
            try:
                message_dict = json.loads(message_value)
                json_data = json.loads(message_dict)

                aqi = json_data["aqi"]
                date_local = json_data["date_local"]
                county = json_data["county"]

                if aqi is not None:
                    if 100 < aqi and aqi < 150:
                        print(f"{date_local} {county} | ALERT: AQI {aqi}, UNHEALTHY FOR SENSITIVE INDIVIDUALS")
                    elif 151 < aqi and aqi < 200:
                        print(f"{date_local} {county} | ALERT: AQI {aqi}, UNHEALTHY")
                    elif 201 < aqi and aqi < 300:
                        print(f"{date_local} {county} | ALERT: AQI {aqi}, VERY UNHEALTHY")
                    elif 301 < aqi:
                        print(f"{date_local} {county} | ALERT: AQI {aqi}, HAZARDOUS")
                
            except json.JSONDecodeError:
                print(f"Failed to decode message as JSON: {message_value}")

except KeyboardInterrupt:
    print("Terminating consumer...")

finally:

    consumer.close()
