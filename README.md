# BayGuard: Real-Time Water Quality Monitoring and Alert System

## Overview

**BayGuard** is a distributed, real-time system designed to monitor water quality from USGS sensors across the San Francisco Bay. It detects pollution threshold violations, validates alerts across nearby sensors, and uses AI (LLM-based) reasoning to verify potential industrial pollution events.

Key technologies include **Apache Kafka**, **Redis**, **Streamlit**, **OpenAI GPT-4**, and an implementation of the **DGIM streaming algorithm**.

---

## Project Architecture

- **WaterDataReader.py**: Harvests real-time water quality data (temperature, turbidity, specific conductance) from USGS APIs. Applies optional differential privacy (DP) noise and publishes normalized records to Kafka.

- **S1Consumer.py**: Consumes water quality data from Kafka, uses DGIM algorithm to detect threshold violations over sliding windows, and applies **Locality-Sensitive Hashing (LSH)** to validate pollution events across multiple sensors.

- **S2Verifier.py**: Listens for alerts on Kafka, then uses **OpenAI GPT-4** to evaluate and reason whether a pollution event is likely industrial pollution.

- **BayGuardUI.py**: Real-time **Streamlit dashboard** that displays the latest alerts and plots sensor locations on an interactive map.

- **dgim.py**: Implements the **DGIM (Datar-Gionis-Indyk-Motwani)** algorithm for efficient streaming window analytics.

- **docker-compose.yml**: Spins up Kafka and Zookeeper locally for testing and development.

---

## Key Features

- ✨ **Real-time Monitoring**: Pulls and analyzes live sensor data every 15 minutes.
- 🔍 **Threshold Violation Detection**: Triggers alerts based on temperature, conductance, and turbidity thresholds.
- 🔄 **Cross-Site Validation**: Uses LSH (approximate nearest neighbors) to validate if pollution events occur across multiple sensors.
- 🧪 **AI Verification**: AI agent (GPT-4) evaluates alerts and suggests confidence levels for true pollution events.
- 📉 **Streaming Dashboard**: Visualizes water quality alerts with interactive maps and real-time updates.
- 🔐 **Differential Privacy Support**: Optional location perturbation for enhanced privacy.

---

## Installation and Setup

### 1. Prerequisites

- Python 3.8+
- Docker and Docker Compose

### 2. Clone the Repository

```bash
git clone https://github.com/mrcyrilgoud/BayGuard.git
cd BayGuard
```

### 3. Environment Variables

Create a `.env` file in the project root:

```dotenv
KAFKA_BROKER=localhost:9092
SOURCE_TOPIC=water_quality
ALERT_TOPIC=water_quality_alerts
TEMP_THRESHOLD=3.0
COND_THRESHOLD=1000.0
TURB_THRESHOLD=10.0
REDIS_HOST=localhost
REDIS_PORT=6379
OPENAI_API_KEY=your_openai_api_key
ENABLE_DP=true
SENSOR_IDS=375603122254401,375607122264701,374811122235001,374938122251801
```

### 4. Start Kafka and Zookeeper

```bash
docker-compose up -d
```

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

(You may need to manually install: `confluent_kafka`, `redis`, `streamlit`, `folium`, `scikit-learn`, `python-dotenv`, `openai`, `backoff`, etc.)

### 6. Run the Services

In separate terminals or background sessions:

```bash
# Start Water Data Producer
python WaterDataReader.py

# Start Stage 1 Consumer (Threshold Checker)
python S1Consumer.py

# Start Stage 2 Verifier (AI Reasoning)
python S2Verifier.py

# Launch Streamlit UI
streamlit run BayGuardUI.py
```

---

## System Diagram

```plaintext
[WaterDataReader] --> (Kafka: water_quality topic)
    |
    v
[S1Consumer] --> (Kafka: water_quality_alerts topic)
    |
    +--> [S2Verifier (GPT-4 Analysis)]
    +--> [BayGuardUI (Streamlit Real-time Dashboard)]
```

---

## Technologies Used

- **Kafka** for messaging
- **Redis** for fast caching and DGIM metric storage
- **Python** for data processing
- **Streamlit + Folium** for visualization
- **OpenAI API** for alert reasoning
- **Docker** for local Kafka/Zookeeper

---

## References

- DGIM Algorithm: [Datar et al., 2002](https://doi.org/10.1137/S0097539701398375)
- Stanford MMDS Book: [Mining of Massive Datasets - Chapter 4](http://infolab.stanford.edu/~ullman/mmds/ch4.pdf)
- USGS Water Services: [NWIS Web Services](https://waterservices.usgs.gov/)
- OpenAI API Documentation: [OpenAI Docs](https://platform.openai.com/docs)

---

## License

This project is licensed under the MIT License. See `LICENSE` file for details.

---

## Acknowledgements

Project was worked on by Cyril Bhoomagoud, Albert Ong, and David Thach
