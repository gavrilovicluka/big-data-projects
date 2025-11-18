FROM bde2020/spark-python-template:3.1.2-hadoop3.2

# ENV PYTHONUNBUFFERED=1

RUN apk add --no-cache py3-numpy

# WORKDIR /app

# COPY spark_streaming.py .

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/spark_streaming.py
ENV SPARK_APPLICATION_ARGS "--window_duration 30 --window_type tumbling"
ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2

ENV KAFKA_HOST=kafka:9092
ENV INPUT_TOPIC=flights_topic
ENV OUTPUT_DIR=/results
ENV MODEL_PATH=/model/lr_pipeline_model
