FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV PYTHONUNBUFFERED=1
ENV KAFKA_HOST=kafka:9092
ENV INPUT_TOPIC=flights_topic
ENV OUTPUT_TOPIC=statistics_topic

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/consumer_spark.py
ENV SPARK_APPLICATION_ARGS "--window_duration 10 --window_type tumbling --target_col CONCURRENT_FLIGHTS --group_by DEP_DEL15"

ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0

ENV CASSANDRA_HOST cassandra
ENV CASSANDRA_PORT 9042

COPY spark-streaming-consumer/consumer_spark.py /app/consumer_spark.py