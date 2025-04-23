import streamlit as st
import redis
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
ALERT_LOG_KEY = os.getenv("ALERT_LOG_KEY", "alert_logs")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

st.set_page_config(page_title="BayGuard Alerts", layout="wide")
st.title("🚨 BayGuard Real-Time Water Quality Alerts")

# Auto-refresh every 10 seconds
st.experimental_rerun_interval = 10

# Load alerts
alerts = r.lrange(ALERT_LOG_KEY, 0, 9)

# Helper to get site name from Redis
@st.cache_data(ttl=60)
def get_site_name(site_code):
    return r.get(f"{site_code}:site_name") or "Unknown Site"

if not alerts:
    st.info("No alerts yet.")
else:
    for i, raw_alert in enumerate(alerts):
        try:
            alert = json.loads(raw_alert)
            alert_type = alert.get("type")
            timestamp = alert.get("timestamp", "N/A")
            st.subheader(f"#{i+1}: {alert_type} @ {timestamp}")

            if alert_type == "UnifiedThresholdAlert":
                site_code = alert.get("sensor")
                site_name = alert.get("site_name") or get_site_name(site_code)
                st.markdown(f"**Sensor:** {site_code}, **Site:** {site_name}")
                st.markdown(f"**Lat/Lon:** {alert.get('latitude')}, {alert.get('longitude')}")
                st.json(alert.get("metrics", {}))

            elif alert_type == "CrossSiteValidatedAlert":
                group = alert.get("group_sites", [])
                st.markdown("**Validated Group Sites:**")
                for sid in group:
                    name = get_site_name(sid)
                    st.markdown(f"- {sid} — {name}")

                st.markdown("**LSH Buckets Used:**")
                st.json(alert.get("lsh_buckets", {}))

            st.markdown("---")
        except Exception as e:
            st.warning(f"Failed to parse alert: {e}")

st.caption("BayGuard | Streamlit Real-Time Dashboard")
