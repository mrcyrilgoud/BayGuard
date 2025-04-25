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
from streamlit_folium import folium_static
import folium
alerts = r.lrange(ALERT_LOG_KEY, 0, 9)

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

def resolve_site_info(site_code, alert=None):
    return {
        "site_name": alert.get("site_name") if alert and alert.get("site_name") else SITE_METADATA.get(site_code, {}).get("site_name", "Unknown Site"),
        "latitude": alert.get("latitude") if alert and alert.get("latitude") else SITE_METADATA.get(site_code, {}).get("latitude", "Unknown"),
        "longitude": alert.get("longitude") if alert and alert.get("longitude") else SITE_METADATA.get(site_code, {}).get("longitude", "Unknown")
    }

# Prepare map
alert_map = folium.Map(location=[37.8, -122.4], zoom_start=9)
marker_count = 0

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
                site_info = resolve_site_info(site_code, alert)
                st.markdown(f"**Sensor:** {site_code}, **Site:** {site_info['site_name']}")
                st.markdown(f"**Lat/Lon:** {site_info['latitude']}, {site_info['longitude']}")
                st.json(alert.get("metrics", {}))

                try:
                    lat = float(site_info['latitude'])
                    lon = float(site_info['longitude'])
                    label = f"{site_info['site_name']} ({site_code}) - {alert_type}"
                    folium.Marker(location=[lat, lon], popup=label, icon=folium.Icon(color='orange')).add_to(alert_map)
                    marker_count += 1
                except:
                    pass

            elif alert_type == "CrossSiteValidatedAlert":
                group = alert.get("group_sites", [])
                st.markdown("**Validated Group Sites:**")
                for sid in group:
                    site_info = resolve_site_info(sid, alert if sid == alert.get("sensor") else {})
                    st.markdown(f"- {sid} — {site_info['site_name']}")

                st.markdown("**LSH Buckets Used:**")
                st.json(alert.get("lsh_buckets", {}))

                try:
                    site_code = group[0] if group else ""
                    site_info = resolve_site_info(site_code, alert if site_code == alert.get("sensor") else {})
                    lat = float(site_info['latitude'])
                    lon = float(site_info['longitude'])
                    label = f"{site_info['site_name']} ({site_code}) - {alert_type}"
                    folium.Marker(location=[lat, lon], popup=label, icon=folium.Icon(color='red')).add_to(alert_map)
                    marker_count += 1
                except:
                    pass

            st.markdown("---")
        except Exception as e:
            st.warning(f"Failed to parse alert: {e}")

if marker_count > 0:
    st.subheader("🗼 Alert Map")
    folium_static(alert_map, width=900, height=500)

st.caption("BayGuard | Streamlit Real-Time Dashboard")

