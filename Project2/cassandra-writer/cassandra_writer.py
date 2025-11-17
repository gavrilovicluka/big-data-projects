import json
import os
import time
from kafka import KafkaConsumer, errors
from cassandra.cluster import Cluster, NoHostAvailable


# -------------------------------
# ENV variables
# -------------------------------
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
KAFKA_TOPIC = os.getenv("OUTPUT_TOPIC", "statistics_topic")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("KEYSPACE", "flights")
TABLE = os.getenv("TABLE", "statistics")


# -------------------------------
# Kafka consumer
# -------------------------------
while True:
    try: 
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_HOST],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        print(f"[cassandra-writer] Connected to Kafka on {KAFKA_TOPIC}...")
        break
    except errors.NoBrokersAvailable:
        print(f"[cassandra-writer] Kafka not available yet. Retrying in 5s...")
        time.sleep(5)


# -------------------------------
# Cassandra connection
# -------------------------------
cluster = None
session = None

while True:
    try:
        print("[cassandra-writer] Trying to connect to Cassandra...")
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        print("[cassandra-writer] Connected to Cassandra.")
        break
    except NoHostAvailable as e:
        print(f"[cassandra-writer] Cassandra not ready. Retrying in 5s...")
        time.sleep(5)


# -------------------------------
# Create keyspace if it does not exist
# -------------------------------
session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")

session.set_keyspace(KEYSPACE)


# -------------------------------
# Create table if it does not exist
# -------------------------------
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        window_start text,
        window_end text,
        group_value text,
        min_value float,
        max_value float,
        avg_value float,
        std_value float,
        PRIMARY KEY (window_start, group_value)
    )
""")

print("Cassandra writer started. Waiting for messages...")


# -------------------------------
# Insert data in table
# -------------------------------
for msg in consumer:
    record = msg.value

    stats = record.get("statistics", {})

    session.execute(
        f"""
        INSERT INTO {TABLE}
        (
            window_start, 
            window_end, 
            group_value,
            min_value, 
            max_value,
            avg_value, 
            std_value
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            record.get("window_start"),
            record.get("window_end"),
            record.get("group"),
            stats.get("min_value"),
            stats.get("max_value"),
            stats.get("avg_value"),
            stats.get("std_value")
        )
    )

    print("Inserted:", record)
