import time
import os
import csv
import json
from kafka import KafkaProducer, errors


# -------------------------------
# ENV variables
# -------------------------------
DATA = os.getenv("DATA", "/data/full_data_flightdelay.csv")
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights_topic")
KAFKA_INTERVAL = os.getenv("KAFKA_INTERVAL", 1)


# -------------------------------
# Kafka producer
# -------------------------------
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_HOST],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Connected to Kafka!")
        break
    except errors.NoBrokersAvailable:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)


# -------------------------------
# Start streaming
# -------------------------------
while True:
    with open(DATA, "r") as file:
        reader = csv.reader(file, delimiter=",")
        headers = next(reader)

        for row in reader:
            value = {headers[i]: row[i] for i in range(len(headers))}

            numeric_int = [
                "MONTH", "DAY_OF_WEEK", "DEP_DEL15", "DISTANCE_GROUP",
                "SEGMENT_NUMBER", "CONCURRENT_FLIGHTS", "NUMBER_OF_SEATS",
                "AIRPORT_FLIGHTS_MONTH", "AIRLINE_FLIGHTS_MONTH",
                "AIRLINE_AIRPORT_FLIGHTS_MONTH", "AVG_MONTHLY_PASS_AIRPORT",
                "AVG_MONTHLY_PASS_AIRLINE", "PLANE_AGE", "TMAX", "AWND"
            ]

            numeric_float = [
                "FLT_ATTENDANTS_PER_PASS", "GROUND_SERV_PER_PASS",
                "PRCP", "SNOW", "SNWD", "LATITUDE", "LONGITUDE"
            ]

            for col in numeric_int:
                if col in value and value[col] != "":
                    value[col] = int(float(value[col]))

            for col in numeric_float:
                if col in value and value[col] != "":
                    value[col] = float(value[col])

            value["timestamp"] = int(time.time())

            print("Sending:", value)
            try:
                producer.send(KAFKA_TOPIC, value=value)
            except Exception as e:
                print(f"Error sending message: {e}")

            time.sleep(float(KAFKA_INTERVAL))