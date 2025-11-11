import time
import os
import csv
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[os.environ["KAFKA_HOST"]],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 11),
)

while True:
    with open(os.environ["DATA"], "r") as file:
        reader = csv.reader(file, delimiter=",")
        headers = next(reader)
        for row in reader:
            value = {headers[i]: row[i] for i in range(len(headers))}

            value["DEP_DELAY"] = float(value["DEP_DELAY"])
            value["ARR_DELAY"] = float(value["ARR_DELAY"])
            value["CANCELLED"] = int(value["CANCELLED"])
            value["DIVERTED"] = int(value["DIVERTED"])
            value["DISTANCE"] = float(value["DISTANCE"])

            value["timestamp"] = int(time.time())

            print(value)
            try:
                producer.send(os.environ["KAFKA_TOPIC"], value=value)
            except Exception as e:
                print(f"Error sending message: {e}")

            time.sleep(float(os.environ["KAFKA_INTERVAL"]))