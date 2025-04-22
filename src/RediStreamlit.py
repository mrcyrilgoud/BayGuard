import streamlit as st
import redis
import os
from dotenv import load_dotenv
from dgim import Dgim
import pickle

# Load .env
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", 4))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

st.title("🔍 Redis Water Quality Viewer")

# Fetch all keys
keys = [k for k in r.keys("*:*") if b":" in k.encode()]
site_keys = sorted(set(k.split(":")[0] for k in keys))

site = st.selectbox("Choose a site code:", site_keys)
metrics = ["temperature", "specific_conductance", "turbidity"]

for metric in metrics:
    key = f"{site}:{metric}"
    values = r.lrange(key, 0, WINDOW_SIZE - 1)
    st.subheader(f"{metric.title()} ({key})")
    st.write(values)
    st.line_chart([float(v) for v in values if v])

    # Reconstruct DGIM for this metric from values
    dgim = Dgim(N=WINDOW_SIZE)
    threshold_key = f"{metric.upper()}_THRESHOLD"
    threshold = float(os.getenv(threshold_key, 0))
    for val in values:
        try:
            dgim.update(float(val) > threshold)
        except:
            pass

    st.markdown(f"**DGIM-estimated violations (last {WINDOW_SIZE} samples)**: {dgim.get_count()}")

st.markdown("---")
st.caption("Made for BayGuard real-time Redis + DGIM monitoring")
