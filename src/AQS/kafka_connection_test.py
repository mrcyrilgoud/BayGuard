"""
Created 2025.04.02

Testing sending a message to Kafka
"""

from confluent_kafka import Producer

KAFKA_BROKER = 'localhost:9092' 
TOPIC = 'test-topic'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err:
        print(f"Error delivering message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} partition {msg.partition()}")

def send_message(message):
    producer.produce(TOPIC, value=message, callback=delivery_report)
    producer.flush()  

if __name__ == '__main__':
    message = "Hello, Kafka! This is a test message."
    print(f"Sending message: {message}")
    send_message(message)
