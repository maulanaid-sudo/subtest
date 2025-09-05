import json
import csv
import os
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from zoneinfo import ZoneInfo


# ---- Konfigurasi MQTT ----
BROKER = "broker.emqx.io"
PORT = 1883
TOPIC = "solchap/level/863353078560355"
USERNAME = "wateriq"
PASSWORD = "wateriq"

# ---- Konfigurasi CSV ----
CSV_FILE = "sensor_data.csv"
CSV_HEADER = ["timestamp_recv", "send_interval", "csq", "battery", "temp", "alarm", "timestamp_device", "water_level"]

# ---- Konfigurasi InfluxDB ----
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "iwQEKouSGB8wjRyrd-1neij0YsLaD-CFvj6rnM8NnOHMSlFZjL2ug142_pQM-BCFONRhzL1vYNXVJ0wMOPu4Cg=="        # ganti sesuai konfigurasi
INFLUX_ORG = "IoT Engineer"
INFLUX_BUCKET = "f838ae2ebed4d702"

client_influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)


# Buat file CSV kalau belum ada
if not os.path.isfile(CSV_FILE):
    with open(CSV_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADER)

# ---- Callback MQTT ----
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())

    # Ambil nilai DevStas
    devstas = payload["params"]["DevStas"]["value"].split(",")
    send_interval, csq, battery, temp = devstas

    # Ambil nilai CM3
    cm5 = payload["params"]["CM5"]["value"].split(",")
    alarm, timestamp_device, water_level = cm5

    # Timestamp waktu pesan diterima (pakai waktu server)

    #timestamp_recv = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    timestamp_recv = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

    # Cetak ke console
    print(f"{'Recv Time':<20}{'Send Interval':<15}{'CSQ':<8}{'Battery(V)':<12}{'Temp(°C)':<10}{'Alarm':<8}{'Device Time':<25}{'Water Level(m)':<15}")
    print(f"{timestamp_recv:<20}{send_interval:<15}{csq:<8}{battery:<12}{temp:<10}{alarm:<8}{timestamp_device:<25}{water_level:<15}")
    print("-"*120)

    # Simpan ke CSV
    with open(CSV_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([timestamp_recv, send_interval, csq, battery, temp, alarm, timestamp_device, water_level])

    # Simpan ke InfluxDB (pakai waktu server)
    point = (
        Point("sensor_data")
        .tag("device_id", payload["id"])
        .field("send_interval", int(send_interval))
        .field("csq", int(csq))
        .field("battery", float(battery))
        .field("temperature", float(temp))
        .field("alarm", int(alarm))
        .field("water_level", float(water_level))
        .field("timestamp_local", str(timestamp_recv))
        #.time(datetime.now(timezone.utc), WritePrecision.S)  # gunakan waktu server
        .time(datetime.now(ZoneInfo("Asia/Shanghai")), WritePrecision.S)
    )
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

# ---- Setup MQTT ----
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(USERNAME, PASSWORD)
mqtt_client.on_message = on_message
mqtt_client.connect(BROKER, PORT, 60)
mqtt_client.subscribe(TOPIC)

print("🚀 Subscribed to MQTT broker with username/password...")
mqtt_client.loop_forever()