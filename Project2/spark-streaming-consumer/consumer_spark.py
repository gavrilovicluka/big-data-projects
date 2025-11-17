import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import *
from pyspark.sql.functions import mean, min, max, col, stddev, to_json, struct, round, count, expr

APP_NAME = "FlightDelaysStreaming"
WINDOW_UNIT = "seconds"

# -------------------------------
# ENV variables
# -------------------------------
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "flights_topic")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "statistics_topic")


def is_float(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


# -------------------------------
# Application arguments
# -------------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window_duration", type=int, required=True, help='Window duration in seconds')
    parser.add_argument("--window_type", required=True, choices=["tumbling", "sliding"])
    parser.add_argument("--slide_duration", default=None, help="Slide duration for sliding window")
    
    parser.add_argument("--group_by", required=True, help="Group by column, e.g., CARRIER_NAME or DEPARTING_AIRPORT")
    parser.add_argument("--filter", action="append", default=[])
    parser.add_argument("--target_col", default="DEP_DEL15", help="Target metric column, e.g., DEP_DEL15")

    return parser.parse_args()


# -------------------------------
# Spark session
# -------------------------------
def initialize_spark():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("[consumer] Spark session initialized.")
    return spark


# -------------------------------
# Dataset schema
# -------------------------------
def get_schema():
    return StructType([
        StructField("MONTH", IntegerType()),
        StructField("DAY_OF_WEEK", IntegerType()),
        StructField("DEP_DEL15", IntegerType()),
        StructField("DISTANCE_GROUP", IntegerType()),
        StructField("DEP_BLOCK", StringType()),
        StructField("SEGMENT_NUMBER", IntegerType()),
        StructField("CONCURRENT_FLIGHTS", IntegerType()),
        StructField("NUMBER_OF_SEATS", IntegerType()),
        StructField("CARRIER_NAME", StringType()),
        StructField("AIRPORT_FLIGHTS_MONTH", IntegerType()),
        StructField("AIRLINE_FLIGHTS_MONTH", IntegerType()),
        StructField("AIRLINE_AIRPORT_FLIGHTS_MONTH", IntegerType()),
        StructField("AVG_MONTHLY_PASS_AIRPORT", IntegerType()),
        StructField("AVG_MONTHLY_PASS_AIRLINE", IntegerType()),
        StructField("FLT_ATTENDANTS_PER_PASS", FloatType()),
        StructField("GROUND_SERV_PER_PASS", FloatType()),
        StructField("PLANE_AGE", IntegerType()),
        StructField("DEPARTING_AIRPORT", StringType()),
        StructField("LATITUDE", FloatType()),
        StructField("LONGITUDE", FloatType()),
        StructField("PREVIOUS_AIRPORT", StringType()),
        StructField("PRCP", FloatType()),
        StructField("SNOW", FloatType()),
        StructField("SNWD", FloatType()),
        StructField("TMAX", IntegerType()),
        StructField("AWND", IntegerType()),
        StructField("timestamp", TimestampType())
    ])


# -------------------------------
# Read Kafka stream
# -------------------------------
def parse_kafka_stream(spark):
    print(f"[consumer] Connecting to Kafka topic {INPUT_TOPIC}...")
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    schema = get_schema()

    # Parse message from JSON into Spark DataFrame
    df = raw.select("timestamp", from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    print("[consumer] Kafka stream parsed.")
    return df


# -------------------------------
# Filters
# -------------------------------
def apply_filters(df, filters):
    for f in filters:
        df = df.filter(expr(f))  # e.g. "PRCP>0"
    return df


# -------------------------------
# Statistics
# -------------------------------
def compute_statistics(df, args):
    if args.window_type == "tumbling":
        slide = args.window_duration
    else:
        slide = args.slide_duration or args.window_duration

    window_duration_str = f"{args.window_duration} {WINDOW_UNIT}"
    slide_duration_str = f"{slide} {WINDOW_UNIT}"

    stats_df = df.groupBy(
        window(col("timestamp"), window_duration_str, slide_duration_str),
        col(args.group_by)
    ).agg(
        round(min(args.target_col), 2).alias("min_value"),
        round(max(args.target_col), 2).alias("max_value"),
        round(mean(args.target_col), 2).alias("avg_value"),
        round(stddev(args.target_col), 2).alias("std_value"),
        count(args.target_col).alias("count")
    )

    return stats_df


# -------------------------------
# Send data to Kafka output topic
# -------------------------------
def write_to_kafka(df):
    # DataFrame: window.start | window.end | group | min_value | max_value | avg_value | std_value | count
    final_df = df.select(
        to_json(
            struct(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col(df.columns[1]).alias("group"),
                struct(
                    "min_value",
                    "max_value",
                    "avg_value",
                    "std_value",
                    "count"
                ).alias("statistics")
            )
        ).alias("value")
    )

    print(f"[consumer] Writing statistics to Kafka topic {OUTPUT_TOPIC}...")
    print(f"[consumer] DataFrame: ", final_df)

    final_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .outputMode("update") \
        .start() \
        .awaitTermination()
    

# -------------------------------
# Main
# -------------------------------
if __name__ == '__main__':
    args = parse_args()
    print(f"[consumer] Started with arguments: {args}")
    
    spark = initialize_spark()
    df_stream = parse_kafka_stream(spark)

    df_filtered = apply_filters(df_stream, args.filter)
    df_stats = compute_statistics(df_filtered, args)

    write_to_kafka(df_stats)


# spark-submit spark_consumer.py \
#     --window_duration 60 \
#     --window_type tumbling \
#     --group_by DEPARTING_AIRPORT \
#     --target_col PLANE_AGE \
#     --filter "CARRIER_NAME=='Delta'" \
#     --filter "DEP_DEL15==1"