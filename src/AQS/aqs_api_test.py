"""
Created: 2025.04.02

Used to connect to the AirQuality System API on the EPA website
"""

import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("AQS_API_KEY")
EMAIL = os.getenv("EMAIL")

if not API_KEY or not EMAIL:
    print("AQS_API_KEY or EMAIL is missing in the .env file")
    exit(1)

BASE_URL = "https://aqs.epa.gov/data/api/"

def api_connection():
    endpoint = "dailyData/byCounty"
    params = {
        "email": EMAIL,
        "key": API_KEY,
        "state": "06", # California
        "county": "085", # Santa Clara
        # "cbsa": "41940", # San Jose-Sunnyvale-Santa Clara 
        # "site": "0100", # San Jose, 4th street
        "param": "88101",
        "bdate": "20240101", 
        "edate": "20240101"
    }
    
    response = requests.get(BASE_URL + endpoint, params=params)
    
    if response.status_code == 200:
        print("Connection successful!")
        print("Response data:", response.json())
        data = response.json().get('Data', [])
        for x in data:
            print(x)
    else:
        print(f"Failed to connect: {response.status_code}")
        print("Error message:", response.text)

if __name__ == "__main__":
    api_connection()
