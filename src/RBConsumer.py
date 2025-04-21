import json
from confluent_kafka import Consumer, Producer
from dgim import Dgim  # Using Simon Dollé's DGIM class
import os
from dotenv import load_dotenv

# Load .env config
load_dotenv()

# Constants from .env
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ALERT_TOPIC = os.getenv("ALERT_TOPIC")

thresholds = {
    "turbidity": float(os.getenv("TURBIDITY_THRESHOLD", 10.0)),
    "temperature": float(os.getenv("TEMPERATURE_THRESHOLD", 25.0)),
    "specific_conductance": float(os.getenv("CONDUCTANCE_THRESHOLD", 800.0)),
    "spike_limit": int(os.getenv("SPIKE_LIMIT", 3))
}

# Initialize DGIM for each metric
WINDOW_SIZE = 4  # last 1 hour if sampling every 15 min
dgim_turbidity = Dgim(N=WINDOW_SIZE)
dgim_temperature = Dgim(N=WINDOW_SIZE)
dgim_conductance = Dgim(N=WINDOW_SIZE)

# Store recent values to compute simple averages
turbidity_vals = []
temperature_vals = []
conductance_vals = []


def create_consumer():
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'water-quality-consumer-group',
        'auto.offset.reset': 'earliest'
    })

def create_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKER})

producer = create_producer()

def process_message(message):
    try:
        data = json.loads(message.value().decode('utf-8'))
        turbidity = float(data.get("turbidity", 0))
        temperature = float(data.get("temperature", 0))
        conductance = float(data.get("specific_conductance", 0))
        site_code = data.get("site_code", "unknown")
        site_name = data.get("site_name", "unknown")
        latitude = data.get("latitude", None)
        longitude = data.get("longitude", None)
        timestamp = data.get("timestamp", "unknown")

        print(f"[INFO] Readings -> Turbidity: {turbidity}, Temperature: {temperature}, Conductance: {conductance}")

        # Keep track of recent values for averaging
        turbidity_vals.append(turbidity)
        temperature_vals.append(temperature)
        conductance_vals.append(conductance)

        if len(turbidity_vals) > WINDOW_SIZE:
            turbidity_vals.pop(0)
            temperature_vals.pop(0)
            conductance_vals.pop(0)

        # Update DGIM with boolean threshold check
        dgim_turbidity.update(turbidity > thresholds["turbidity"])
        dgim_temperature.update(temperature > thresholds["temperature"])
        dgim_conductance.update(conductance > thresholds["specific_conductance"])

        # Estimate high-value occurrences in the past hour
        turbidity_spikes = dgim_turbidity.get_count()
        temperature_spikes = dgim_temperature.get_count()
        conductance_spikes = dgim_conductance.get_count()

        print(f"[INFO] Recent spikes -> Turbidity: {turbidity_spikes}, Temperature: {temperature_spikes}, Conductance: {conductance_spikes}")

        alerts = []
        if turbidity_spikes >= thresholds["spike_limit"]:
            alerts.append("turbidity")
        if temperature_spikes >= thresholds["spike_limit"]:
            alerts.append("temperature")
        if conductance_spikes >= thresholds["spike_limit"]:
            alerts.append("specific_conductance")

        if alerts:
            metric_averages = {
                "turbidity": sum(turbidity_vals) / len(turbidity_vals),
                "temperature": sum(temperature_vals) / len(temperature_vals),
                "specific_conductance": sum(conductance_vals) / len(conductance_vals)
            }

            alert_msg = {
                "type": "UnifiedThresholdAlert",
                "metrics": {k: round(metric_averages[k], 2) for k in alerts},
                "source": "RBConsumer.py",
                "sensor": site_code,
                "site_name": site_name,
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": timestamp
            }

            print("\n⚠️ Unified Alert: One or more parameters have exceeded safe levels:")
            print(json.dumps(alert_msg, indent=2))

            producer.produce(ALERT_TOPIC, json.dumps(alert_msg).encode('utf-8'))
            producer.flush()

    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")

def main():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    print("[STARTED] Listening to Kafka topic for water quality data...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            process_message(msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()


