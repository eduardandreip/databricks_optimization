# Databricks notebook source
import pandas as pd
from datetime import datetime
import requests
import json

# Replace these values with your own Databricks information
DATABRICKS_DOMAIN = "https://adb-xxxxxxxxxxxxxxxx.xx.azuredatabricks.net"
DATABRICKS_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
CLUSTER_ID = "0000-000000-XXXXXXXX"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
}

payload = json.dumps(
    {
        "cluster_id": CLUSTER_ID,
    }
)

url = f"{DATABRICKS_DOMAIN}/api/2.0/clusters/events"

response = requests.post(url, headers=headers, data=payload)

# COMMAND ----------

json_data = response.json()

events = json_data["events"]

# Create a DataFrame from the events list and select the required columns
df = pd.DataFrame(events)[['timestamp', 'cluster_id', 'type', 'details']]
df['reason'] = df['details'].apply(lambda x: x.get('reason', None))
df['code'] = df['reason'].apply(lambda x: x.get('code', None) if x else None)
df = df[["timestamp","cluster_id","type","code"]]
# Convert Unix timestamps to readable timestamps
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df['inactivity_count'] = df['code'].apply(lambda x:1 if x == "INACTIVITY" else 0)
df

# COMMAND ----------

spdf = spark.createDataFrame(df)
spdf.display()
