FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/train_model.py
ENV DATA_PATH=/data/full_data_flightdelay.csv 
ENV MODEL_PATH=/model/lr_pipeline_model