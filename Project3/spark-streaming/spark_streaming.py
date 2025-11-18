import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import *
from pyspark.sql.functions import col, round, count, lit, when, sum, max
from pyspark.ml import PipelineModel

APP_NAME = "FlightDelayPredictionStreaming"
WINDOW_UNIT = "seconds"

# -------------------------------
# ENV variables
# -------------------------------
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "flights_topic")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output/predictions")
MODEL_PATH = os.getenv("MODEL_PATH", "/model/lr_pipeline_model")


# -------------------------------
# Columns used by model (must match the values used for training)
# -------------------------------
NUMERIC_FEATURES = [
    "MONTH", "DAY_OF_WEEK", "DISTANCE_GROUP",
    "CONCURRENT_FLIGHTS", "AIRPORT_FLIGHTS_MONTH",
    "AIRLINE_FLIGHTS_MONTH", "AVG_MONTHLY_PASS_AIRPORT", 
    "AVG_MONTHLY_PASS_AIRLINE", "PLANE_AGE",
    "TMAX", "AWND", "LATITUDE", "LONGITUDE"
]
TARGET_COL = "DEP_DEL15"
CATEGORICAL_FEATURES = ["CARRIER_NAME", "DEPARTING_AIRPORT"]


# -------------------------------
# Application arguments
# -------------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window_duration", type=int, required=True, help='Window duration in seconds')
    parser.add_argument("--window_type", required=True, choices=["tumbling", "sliding"])
    parser.add_argument("--slide_duration", default=None, help="Slide duration for sliding window")

    return parser.parse_args()


# -------------------------------
# Spark session
# -------------------------------
def initialize_spark():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()
    
    print("[streamer] Current Spark master:", spark.sparkContext.master)

    spark.sparkContext.setLogLevel("ERROR")

    print("[streamer] Spark session initialized.")
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
    print(f"[streamer] Connecting to Kafka topic {INPUT_TOPIC}...")
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 50) \
        .load()
    
    schema = get_schema()

    df = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    df = df.withColumn("event_timestamp", col("timestamp").cast("timestamp"))

    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, col(c).cast("double")).na.fill(0.0, subset=[c])
    
    for c in CATEGORICAL_FEATURES:
        df = df.na.fill("N/A", subset=[c])

    print("[streamer] Kafka stream parsed.")
    return df


# -------------------------------
# ML prediction model
# -------------------------------
def apply_ml_prediction(df):
    try:
        print(f"[streamer] Loading ML model from: {MODEL_PATH}...")
        pipeline_model = PipelineModel.load(MODEL_PATH)
    except Exception as e:
        print(f"[streamer] Error while loading model: {e}")
        return df.withColumn("prediction", lit(-1.0))
    
    predictions = pipeline_model.transform(df)
    
    predictions = predictions.withColumnRenamed("prediction", "predicted_delay")

    output_cols = [
        "event_timestamp", TARGET_COL, "predicted_delay", 
        "CARRIER_NAME", "DEPARTING_AIRPORT", "LATITUDE", "LONGITUDE"
    ]
    
    return predictions.select(*output_cols)


# -------------------------------
# Aggregate by time window
# -------------------------------
def aggregate_and_predict_window(df, args):
    window_duration_str = f"{args.window_duration} seconds"

    if args.window_type.lower() == "tumbling":
        window_col = window(col("event_timestamp"), window_duration_str)
    else:
        slide_duration = args.slide_duration or args.window_duration
        slide_duration_str = f"{slide_duration} seconds"
        window_col = window(col("event_timestamp"), window_duration_str, slide_duration_str)

    df_with_watermark = df.withWatermark("event_timestamp", "3 minutes")

    windowed_predictions = df_with_watermark.groupBy(
        window_col,
        col("DEPARTING_AIRPORT").alias("airport_name")
    ).agg(
        count(col(TARGET_COL)).alias("total_flights"),
        sum(col(TARGET_COL)).alias("actual_delays"),
        sum(col("predicted_delay")).alias("predicted_delays_count"),
        
        sum(when(col(TARGET_COL).cast("int") == col("predicted_delay").cast("int"), 1).otherwise(0)).alias("correct_predictions"),

        max(col("LATITUDE")).alias("LATITUDE"),
        max(col("LONGITUDE")).alias("LONGITUDE")
    ).withColumn(
        "accuracy_percent",
        round(col("correct_predictions") / col("total_flights") * 100, 2)
    )

    return windowed_predictions


# -------------------------------
# Write CSV
# -------------------------------
def write_to_file(df):
    output_df = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("airport_name"),
        col("LATITUDE"),
        col("LONGITUDE"),
        col("total_flights"),
        col("actual_delays"),
        col("predicted_delays_count"),
        col("accuracy_percent")
    )

    print(f"[streamer] Starting write of windowed predictions to: {OUTPUT_DIR}...")

    def foreach_batch_function(batch_df, batch_id):
        if batch_df.count() == 0:
            print(f"[streamer] Batch {batch_id} empty, skipping.")
            return
    
        print(f"[streamer] Writing batch {batch_id} to CSV, row count: {batch_df.count()}")

        batch_df.repartition(1).write \
            .mode("append") \
            .option("header", True) \
            .csv(os.path.join(OUTPUT_DIR, f"batch_{batch_id}"))
        
        print(f"[streamer] Batch {batch_id} written.")

    query = output_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .start() \
        # .option("checkpointLocation", "/checkpoint/flight_predictions") \
        
        
        # .outputMode("append") \
        # .format("csv") \
        # .option("path", OUTPUT_DIR) \
        # .option("checkpointLocation", "/tmp/checkpoints_csv") \
        # .option("header", "true") \
        # .start()
        
        # .option("checkpointLocation", "/tmp/checkpoints_csv") \
        # .format("csv") \
        # .option("header", "true") \
        # .option("path", OUTPUT_DIR) \
    
    query.awaitTermination()


# -------------------------------
# Main
# -------------------------------
if __name__ == '__main__':
    args = parse_args()
    print(f"[streamer] Started with arguments: {args}")
    
    spark = initialize_spark()
    df_stream = parse_kafka_stream(spark)

    df_predictions = apply_ml_prediction(df_stream)
    
    df_windowed_stats = aggregate_and_predict_window(df_predictions, args)
    
    write_to_file(df_windowed_stats)
