FROM bde2020/spark-python-template:3.1.2-hadoop3.2

COPY flightDelays.py /app/
COPY utils.py /app/

ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/flightDelays.py
ENV SPARK_SUBMIT_ARGS "--executor-memory 3G --executor-cores 3 --py-files /app/utils.py"
ENV SPARK_APPLICATION_ARGS "--input=hdfs://namenode:9000/dir/full_data_flightdelay.csv --month=7 --carrier=\"United Air Lines Inc.\" --stats_col=PLANE_AGE --group_by_col=CARRIER_NAME"